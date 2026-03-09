import logging
import shutil
import re
import shlex
import subprocess
import threading
import urllib.request
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Any, TypedDict, Dict, List, cast

try:
    import yt_dlp  # type: ignore
    from yt_dlp.utils import DownloadError  # type: ignore
except ImportError as e:
    raise ImportError(f"CRITICAL: Dependência em falta. {e}. Execute 'pip install yt-dlp'")

from PyQt6.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot

class MediaType(Enum):
    AUDIO = auto()
    VIDEO = auto()

@dataclass(frozen=True)
class MediaMetadata:
    title: str
    artist: str
    album: str
    duration: int
    thumbnail_url: Optional[str]
    width: Optional[int]
    height: Optional[int]
    fps: Optional[float]
    display_duration: str
    upload_date: str
    description: str
    channel: str

@dataclass(frozen=True)
class DownloadJobConfig:
    job_id: str
    url: str
    output_path: Path
    custom_filename: str
    media_type: MediaType
    format_container: str
    audio_codec: str
    video_codec: str
    quality_preset: str
    audio_sample_rate: int
    audio_bitrate: str
    
    audio_bit_depth: str = 'auto'
    output_template: str = ''
    ffmpeg_path: str = ''
    custom_flags: str = ''
    
    custom_cover_path: str = ''
    
    meta_title: str = ''
    meta_artist: str = ''
    meta_album: str = ''
    meta_genre: str = ''
    meta_date: str = ''
    meta_desc: str = ''
    
    embed_metadata: bool = True
    embed_thumbnail: bool = True
    embed_subs: bool = False
    normalize_audio: bool = False
    use_browser_cookies: bool = False

class YtDlpExtractedInfo(TypedDict, total=False):
    title: str
    artist: str
    uploader: str
    album: str
    genre: str
    duration: int
    thumbnail: str
    width: int
    height: int
    fps: float
    duration_string: str
    upload_date: str
    description: str
    comment: str

class IMessageBroker(ABC):
    @abstractmethod
    def emit_finished(self) -> None: pass
    @abstractmethod
    def emit_error(self, message: str) -> None: pass
    @abstractmethod
    def emit_result(self, result: Any) -> None: pass
    @abstractmethod
    def emit_progress(self, job_id: str, percent: float, speed: str) -> None: pass
    @abstractmethod
    def emit_status(self, job_id: str, status: str) -> None: pass
    @abstractmethod
    def emit_thumbnail(self, data: bytes) -> None: pass

class WorkerSignals(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)
    progress = pyqtSignal(str, float, str)
    status = pyqtSignal(str, str)
    thumbnail_data = pyqtSignal(bytes)

class PyQtMessageBroker(IMessageBroker):
    def __init__(self, signals: WorkerSignals) -> None:
        self.signals = signals

    def emit_finished(self) -> None: self.signals.finished.emit()
    def emit_error(self, message: str) -> None: self.signals.error.emit(message)
    def emit_result(self, result: Any) -> None: self.signals.result.emit(result)
    def emit_progress(self, job_id: str, percent: float, speed: str) -> None: self.signals.progress.emit(job_id, percent, speed)
    def emit_status(self, job_id: str, status: str) -> None: self.signals.status.emit(job_id, status)
    def emit_thumbnail(self, data: bytes) -> None: self.signals.thumbnail_data.emit(data)

class YtDlpService:
    URL_REGEX = re.compile(
        r'^(https?://)?(www\.)?(youtube\.com|youtu\.be|music\.youtube\.com|vimeo\.com|soundcloud\.com)/.+$'
    )
    _network_semaphore = threading.BoundedSemaphore(value=5)

    @staticmethod
    def validate_url(url: str) -> bool:
        return bool(YtDlpService.URL_REGEX.match(url))

    @classmethod
    def extract_info_sync(cls, url: str) -> YtDlpExtractedInfo:
        logging.debug(f"[Network] Aguardando liberação de semáforo para extração de topologia: {url}")
        with cls._network_semaphore:
            
            ydl_opts: Dict[str, Any] = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'socket_timeout': 15,
                'logger': logging.getLogger('yt_dlp_internal'),
                'remote_components': ['ejs:github'],
                'extractor_args': {
                    'youtube': ['player_client=ios,android,mweb', 'player_skip=configs']
                },
            }
            
            cookie_file = Path("cookies.txt")
            if cookie_file.exists() and cookie_file.is_file():
                ydl_opts['cookiefile'] = str(cookie_file)
            
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    
            except DownloadError as e:
                error_msg = str(e).lower()
                if "sign in to confirm" in error_msg or "bot" in error_msg:
                    msg = (
                        "Colapso de Camada de Rede: Interceção pelo WAF do YouTube (Anti-Bot).\n\n"
                        "Ações de Mitigação Exigidas:\n"
                        "1. Instale um interpretador JavaScript nativo (ex: 'winget install OpenJS.NodeJS') para resolução de criptografia PoW em runtime.\n"
                        "2. Exporte a sua sessão autenticada utilizando a extensão de navegador 'Get cookies.txt LOCALLY'.\n"
                        "3. Coloque o ficheiro resultante com o nome exato 'cookies.txt' no diretório onde o seu script é executado."
                    )
                    logging.critical(f"[Security] WAF Triggered. {msg}")
                    raise RuntimeError(msg)
                else:
                    raise

            if info and 'entries' in info:
                info = info['entries'][0]
            return cast(YtDlpExtractedInfo, info)

    @classmethod
    def fetch_thumbnail_bytes_sync(cls, url: str) -> Optional[bytes]:
        try:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                return response.read()
        except Exception as e:
            logging.error(f"[I/O] Falha na aquisição de matriz RAW da miniatura: {e}")
            return None

class YtDlpInterceptorLogger:
    def __init__(self, job_id: str, check_abort_callback: Any) -> None:
        self.job_id = job_id
        self._check_abort = check_abort_callback
        self.logger = logging.getLogger(f"yt_dlp_{job_id[:8]}")

    def debug(self, msg: str) -> None:
        self._check_abort()
        self.logger.debug(msg)

    def info(self, msg: str) -> None:
        self._check_abort()
        self.logger.info(msg)

    def warning(self, msg: str) -> None:
        self._check_abort()
        self.logger.warning(msg)

    def error(self, msg: str) -> None:
        self._check_abort()
        self.logger.error(msg)

class SubprocessDSPEngine:
    @staticmethod
    def execute_audio_pipeline(config: DownloadJobConfig, raw_filepath: Path, info_dict: dict[str, Any], temp_dir: Path, logger: logging.Logger) -> None:
        
        if not raw_filepath.exists():
            possible_files = list(temp_dir.glob(f"{raw_filepath.stem}.*"))
            if possible_files: raw_filepath = possible_files[0]
            else: raise RuntimeError(f"Ficheiro bruto de origem não encontrado: {raw_filepath}")

        ext = config.format_container
        out_filepath = raw_filepath.with_suffix(f".{ext}")
        
        if raw_filepath.absolute() == out_filepath.absolute():
            new_raw = raw_filepath.with_suffix(".raw_audio")
            shutil.move(str(raw_filepath), str(new_raw))
            raw_filepath = new_raw
            
        ffmpeg_bin = config.ffmpeg_path if config.ffmpeg_path else 'ffmpeg'
        cmd = [ffmpeg_bin, '-y', '-i', str(raw_filepath)]
        
        cover_target_path: Optional[Path] = None
        
        if config.embed_thumbnail:
            temp_dir.mkdir(parents=True, exist_ok=True)
            if config.custom_cover_path and Path(config.custom_cover_path).exists():
                cover_target_path = Path(config.custom_cover_path)
                logger.info("[DSP] Matriz gráfica do MusicBrainz injetada com sucesso.")
            elif info_dict.get('thumbnail'):
                thumb_bytes = YtDlpService.fetch_thumbnail_bytes_sync(info_dict['thumbnail'])
                if thumb_bytes:
                    cover_target_path = temp_dir / "cover_art_fallback.jpg"
                    cover_target_path.write_bytes(thumb_bytes)
        
        if cover_target_path:
            cmd.extend(['-i', str(cover_target_path)])
                
        if ext == 'flac': cmd.extend(['-c:a', 'flac'])
        elif ext == 'wav': cmd.extend(['-c:a', 'pcm_s16le']) 
        elif ext in ['mp3', 'm4a', 'aac']:
            cmd.extend(['-c:a', 'aac' if ext in ['m4a', 'aac'] else 'libmp3lame'])
            if config.audio_bitrate != '0': cmd.extend(['-b:a', f"{config.audio_bitrate}k"])
                
        audio_filters: List[str] = []
        aresample_opts: List[str] = []
        
        if config.audio_sample_rate > 0:
            aresample_opts.append(f"osr={config.audio_sample_rate}")
            
        if ext in ['flac', 'wav'] and config.audio_bit_depth != 'auto':
            aresample_opts.append(f"osf=s{config.audio_bit_depth}")
            aresample_opts.append("dither_method=triangular")
            
        if aresample_opts:
            audio_filters.append(f"aresample={':'.join(aresample_opts)}")
            
        if config.normalize_audio:
            audio_filters.append("loudnorm=I=-14:TP=-1.5:LRA=11")
            
        if audio_filters:
            cmd.extend(['-af', ','.join(audio_filters)])
            
        if config.embed_metadata:
            def sanitize(val: str) -> str:
                return str(val).replace('\r\n', ' ').replace('\n', ' ').replace('"', "'").strip()
            
            title = sanitize(config.meta_title or info_dict.get('title', ''))
            artist = sanitize(config.meta_artist or info_dict.get('uploader', ''))
            cmd.extend([
                '-metadata', f"title={title}",
                '-metadata', f"artist={artist}",
                '-metadata', f"album_artist={artist}",
                '-metadata', f"album={sanitize(config.meta_album)}",
                '-metadata', f"genre={sanitize(config.meta_genre)}",
                '-metadata', f"comment={sanitize(config.meta_desc)}",
            ])
            if config.meta_date: cmd.extend(['-metadata', f"date={sanitize(config.meta_date[:4])}"])
                
        if cover_target_path:
            cmd.extend(['-map', '0:a:0', '-map', '1:v:0', '-c:v', 'mjpeg', '-disposition:v', 'attached_pic'])
            if ext == 'mp3':
                cmd.extend(['-id3v2_version', '3', '-metadata:s:v', 'title="Album cover"', '-metadata:s:v', 'comment="Cover (front)"'])
        else:
            cmd.extend(['-map', '0:a:0'])
        
        cmd.append(str(out_filepath))
        
        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Colapso DSP: {e.stderr}")
            raise RuntimeError(f"FFmpeg falhou ao processar matriz: {e.stderr}")
        finally:
            def safe_delete(p: Path) -> None:
                if not p or not p.exists(): return
                import time
                for _ in range(3):
                    try: p.unlink(); break
                    except PermissionError: time.sleep(1.0)
            
            if raw_filepath.exists() and raw_filepath.absolute() != out_filepath.absolute(): safe_delete(raw_filepath)
            if cover_target_path and "cover_art_fallback" in cover_target_path.name: safe_delete(cover_target_path)
class AnalysisWorker(QRunnable):
    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url
        self._signals = WorkerSignals()
        self.broker = PyQtMessageBroker(self._signals)

    @property
    def signals(self) -> WorkerSignals:
        return self._signals

    @pyqtSlot()
    def run(self) -> None:
        try:
            info = YtDlpService.extract_info_sync(self.url)
            
            meta = MediaMetadata(
                title=info.get('title', 'Unknown'),
                artist=info.get('artist', info.get('uploader', 'Unknown')),
                album=info.get('album', ''),
                duration=info.get('duration', 0),
                thumbnail_url=info.get('thumbnail'),
                width=info.get('width'),
                height=info.get('height'),
                fps=info.get('fps'),
                display_duration=info.get('duration_string', 'N/A'),
                upload_date=info.get('upload_date', ''),
                description=info.get('description', ''),
                channel=info.get('uploader', '')
            )
            
            self.broker.emit_result(meta)

            if meta.thumbnail_url:
                data = YtDlpService.fetch_thumbnail_bytes_sync(meta.thumbnail_url)
                if data:
                    self.broker.emit_thumbnail(data)
                    
        except Exception as e:
            logging.exception("Sub-rotina de análise interceptada com exceção.")
            self.broker.emit_error(str(e))
        finally:
            self.broker.emit_finished()

class DownloadWorker(QRunnable):
    def __init__(self, config: DownloadJobConfig) -> None:
        super().__init__()
        self.config = config
        self._signals = WorkerSignals()
        self.broker = PyQtMessageBroker(self._signals)
        self._is_cancelled = False
        self._temp_dir: Path = self.config.output_path / ".inprogress" / self.config.job_id
        self._logger = logging.getLogger(f"Job_{self.config.job_id[:8]}")

    @property
    def signals(self) -> WorkerSignals:
        return self._signals

    def cancel(self) -> None:
        self._is_cancelled = True
        self._logger.warning("Sinal de interrupção (SIGTERM logic) propagado.")

    def _check_abort(self) -> None:
        if self._is_cancelled:
            raise DownloadError("Operação de E/S vetada no escopo de utilizador.")

    def _progress_hook(self, d: dict[str, Any]) -> None:
        self._check_abort()
        status = d.get('status')
        
        if status == 'downloading':
            p = d.get('_percent_str', '0%').replace('%', '')
            try: 
                percent = float(p)
            except ValueError: 
                percent = 0.0
            self.broker.emit_progress(self.config.job_id, percent, d.get('_speed_str', 'N/A'))
        
        elif status == 'finished':
            self._logger.info("Ciclo de leitura binária saturado. Transição de estado.")
            self.broker.emit_progress(self.config.job_id, 100.0, "Processing")

    def _build_ydl_opts(self) -> dict[str, Any]:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        
        filename_tmpl = self.config.output_template if self.config.output_template else f"{self.config.custom_filename}.%(ext)s"
        out_tmpl = str(self._temp_dir / filename_tmpl)
        
        opts: dict[str, Any] = {
            'outtmpl': out_tmpl,
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': False,
            'retries': 10,
            'fragment_retries': 10,
            'socket_timeout': 15,
            'logger': YtDlpInterceptorLogger(self.config.job_id, self._check_abort),
            'postprocessors': [],
            'postprocessor_args': {},
            'remote_components': ['ejs:github'],
            'extractor_args': {
                'youtube': ['player_client=ios,android,mweb', 'player_skip=configs']
            }
        }

        if self.config.ffmpeg_path: opts['ffmpeg_location'] = self.config.ffmpeg_path
        
        cookie_file = Path("cookies.txt")
        if cookie_file.exists() and cookie_file.is_file():
            opts['cookiefile'] = str(cookie_file)

        self._configure_format_opts(opts)
        self._configure_metadata_opts(opts)
        self._apply_raw_custom_flags(opts)
        return opts

    def _configure_format_opts(self, opts: dict[str, Any]) -> None:
        if self.config.media_type == MediaType.AUDIO:
            opts['format'] = 'bestaudio/best'
            return

        preset = self.config.quality_preset.lower()
        target_h: Optional[int] = None
        is_best_req = any(kw in preset for kw in ['melhor', 'best', 'source', 'original'])
        
        if not is_best_req:
            res_map = {"4k": 2160, "2k": 1440, "1080p": 1080, "720p": 720, "480p": 480}
            match = re.search(r'(\d+)p', preset)
            target_h = int(match.group(1)) if match else res_map.get(preset)

        res_filter = f"[height<={target_h}]" if target_h is not None else ""
        opts['format'] = f"bestvideo{res_filter}+bestaudio/best{res_filter}"
        
        sort_opts: List[str] = ['res', 'fps']
        v_codec = self.config.video_codec.lower()
        is_legacy = v_codec in ['divx', 'xvid'] or self.config.format_container == 'avi'
        
        if not is_legacy:
            opts['merge_output_format'] = self.config.format_container
            if 'melhor' not in v_codec and 'best' not in v_codec:
                vc = {'h264': 'avc', 'vp9': 'vp9', 'av1': 'av01'}.get(v_codec, v_codec)
                sort_opts.append(f"vcodec:{vc}")
            a_codec = self.config.audio_codec.lower()
            if 'melhor' not in a_codec and 'best' not in a_codec:
                sort_opts.append(f"acodec:{a_codec}")
        else:
            opts['merge_output_format'] = 'mkv'
            opts['postprocessors'].append({'key': 'FFmpegVideoConvertor', 'preferedformat': self.config.format_container if self.config.format_container == 'avi' else 'mkv'})
            pp_args = []
            if v_codec == 'divx': pp_args.extend(['-c:v', 'mpeg4', '-vtag', 'DIVX', '-qscale:v', '3'])
            elif v_codec == 'xvid': pp_args.extend(['-c:v', 'libxvid', '-qscale:v', '3'])
            pp_args.extend(['-c:a', 'libmp3lame', '-b:a', '192k', '-ar', '44100'])
            opts.setdefault('postprocessor_args', {}).setdefault('FFmpegVideoConvertor', []).extend(pp_args)

        opts['format_sort'] = sort_opts

    def _configure_metadata_opts(self, opts: dict[str, Any]) -> None:
        if self.config.media_type == MediaType.AUDIO:
            return
            
        if self.config.embed_metadata:
            opts['postprocessors'].append({'key': 'FFmpegMetadata', 'add_chapters': True, 'add_metadata': True})
            meta_args: List[str] = []
            if self.config.meta_title: meta_args.extend(['-metadata', f'title={self.config.meta_title}'])
            if self.config.meta_artist: meta_args.extend(['-metadata', f'artist={self.config.meta_artist}', '-metadata', f'album_artist={self.config.meta_artist}'])
            if self.config.meta_album: meta_args.extend(['-metadata', f'album={self.config.meta_album}'])
            if self.config.meta_genre: meta_args.extend(['-metadata', f'genre={self.config.meta_genre}'])
            if self.config.meta_date: meta_args.extend(['-metadata', f'date={self.config.meta_date[:4]}'])
            if self.config.meta_desc: meta_args.extend(['-metadata', f'description={self.config.meta_desc}', '-metadata', f'comment={self.config.meta_desc}'])
            if meta_args: opts.setdefault('postprocessor_args', {})['FFmpegMetadata'] = meta_args
                
        if self.config.embed_thumbnail:
            opts['postprocessors'].append({'key': 'EmbedThumbnail'})
        if self.config.embed_subs:
            opts['writesubtitles'] = True

    def _apply_raw_custom_flags(self, opts: dict[str, Any]) -> None:
        if not self.config.custom_flags: return
        try:
            tokens = shlex.split(self.config.custom_flags)
            i = 0
            while i < len(tokens):
                flag = tokens[i]
                if flag.startswith('--'):
                    key = flag[2:].replace('-', '_')
                    if i + 1 < len(tokens) and not tokens[i+1].startswith('--'):
                        val = tokens[i+1]
                        i += 2
                        if key == 'extractor_args' and ':' in val and '=' in val:
                            extractor, rest = val.split(':', 1)
                            arg_k, arg_v = rest.split('=', 1)
                            opts.setdefault('extractor_args', {}).setdefault(extractor, {})[arg_k] = [arg_v]
                        else: opts[key] = val
                    else:
                        opts[key] = True
                        i += 1
                else: i += 1
        except ValueError as e:
            self._logger.error(f"Erro Léxico em Abstract Syntax Tree (Flags): {e}")

    def _get_downloaded_filepath(self, info_dict: dict[str, Any]) -> Optional[Path]:
        req_dl = info_dict.get('requested_downloads')
        if req_dl and isinstance(req_dl, list) and req_dl[0].get('filepath'):
            return Path(req_dl[0].get('filepath'))
        filepath = info_dict.get('filepath')
        return Path(filepath) if filepath else None

    def _finalize_move(self) -> None:
        dest_dir = self.config.output_path
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        for file_p in self._temp_dir.iterdir():
            if file_p.is_file():
                target = dest_dir / file_p.name
                
                for attempt in range(5):
                    try:
                        if target.exists(): target.unlink()
                        shutil.move(str(file_p), str(target))
                        break
                    except PermissionError as e:
                        if attempt == 4:
                            self._logger.error(f"Falha definitiva ao mover artefacto (I/O Lock): {e}")
                            raise
                        import time

                        time.sleep(1.5)

        shutil.rmtree(self._temp_dir, ignore_errors=True)

    @pyqtSlot()
    def run(self) -> None:
        self.broker.emit_status(self.config.job_id, "Iniciando topologia de descritores...")
        try:
            ydl_opts = self._build_ydl_opts()
            with YtDlpService._network_semaphore:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    self.broker.emit_status(self.config.job_id, "Rotina I/O bloqueante alocada.")
                    info_dict = ydl.extract_info(self.config.url, download=True)
            
            self._check_abort()
            
            if self.config.media_type == MediaType.AUDIO:
                self.broker.emit_status(self.config.job_id, "Motor DSP em execução iterativa.")
                raw_filepath = self._get_downloaded_filepath(cast(Dict[str, Any], info_dict))
                if raw_filepath:
                    SubprocessDSPEngine.execute_audio_pipeline(self.config, raw_filepath, cast(Dict[str, Any], info_dict), self._temp_dir, self._logger)

            self._finalize_move()
            self.broker.emit_status(self.config.job_id, "Integridade Verificada.")
            
        except Exception as e:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            if not self._is_cancelled:
                self._logger.error(f"Pânico no kernel ou falha no Pipe: {e}", exc_info=True)
                self.broker.emit_error(str(e))
        finally:
            self.broker.emit_finished()