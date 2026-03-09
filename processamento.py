"""
Pipeline de Resolução e Processamento de Mídia (DSP e Extração)

Este módulo implementa a orquestração multithread para extração de topologias de rede
(YouTube, Spotify) e processamento de sinais digitais (DSP) via FFmpeg.

Diretrizes de Segurança e Injeção de Dependências (OAuth2):
-----------------------------------------------------------
Com base nos princípios de isolamento de configuração documentados no manifesto 
The Twelve-Factor App (https://12factor.net/config), o acoplamento de credenciais
estáticas no código-fonte é estritamente vetado. O adaptador `SpotifyToYTMAdapter` 
exige a injeção de credenciais via variáveis de ambiente do Sistema Operacional (OS).

[1] Ambiente de Desenvolvimento (Dev/Local):
    - Windows (PowerShell):
        $env:SPOTIPY_CLIENT_ID="seu_client_id"
        $env:SPOTIPY_CLIENT_SECRET="seu_client_secret"
    - Unix/Linux/macOS (Bash/Zsh):
        export SPOTIPY_CLIENT_ID="seu_client_id"
        export SPOTIPY_CLIENT_SECRET="seu_client_secret"
    - Interpretadores e IDEs (VSCode):
        Alocar o dicionário de variáveis no arquivo `launch.json`:
        "env": {"SPOTIPY_CLIENT_ID": "...", "SPOTIPY_CLIENT_SECRET": "..."}

[2] Ambiente de Produção (Prod):
    - Systemd (Daemons Linux):
        Empregar a diretiva `EnvironmentFile=/etc/sua_app/secrets.env` na unit `.service`
        com permissões restritas de leitura (chmod 600).
    - Infraestrutura Imutável (Docker/Kubernetes):
        Injetar as variáveis em tempo de execução via manifestos YAML (`env:` ou `secretKeyRef:`)
        evitando a persistência de credenciais em imagens de contêiner.
    - Mitigação Avançada: 
        Para sistemas de alta disponibilidade, recomenda-se a alocação dinâmica de chaves 
        em memória utilizando cofres criptográficos efêmeros (e.g., HashiCorp Vault).

Nota de Degradação Graciosa (Graceful Degradation):
A ausência das variáveis supracitadas não inviabiliza a alocação do pipeline. O sistema
isolará a falha ao domínio do Spotify, preservando a integridade das sub-rotinas do YouTube.
"""

import logging
import os
import re
import shlex
import shutil
import ssl
import subprocess
import threading
import urllib.request
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Any, TypedDict, Dict, List, cast

try:
    import yt_dlp  # type: ignore
    from yt_dlp.utils import DownloadError  # type: ignore
except ImportError as e:
    raise ImportError(f"CRITICAL: Dependência yt-dlp não resolvida. {e}")

try:
    import spotipy  # type: ignore
    from spotipy.oauth2 import SpotifyClientCredentials  # type: ignore
except ImportError as e:
    raise ImportError(f"CRITICAL: Dependência spotipy não resolvida. {e}")

from PyQt6.QtCore import QObject, pyqtSignal, QRunnable, pyqtSlot, QThreadPool

class MediaType(Enum):
    AUDIO = auto()
    VIDEO = auto()

@dataclass(frozen=True)
class NormalizedMediaEntity:
    original_id: str
    title: str
    artist: str
    album: str
    duration: int = 0
    thumbnail_url: Optional[str] = None
    is_playlist: bool = False
    children: Optional[List['NormalizedMediaEntity']] = field(default_factory=list)
    
    upload_date: Optional[str] = None
    description: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[float] = None
    channel: Optional[str] = None

    @property
    def display_duration(self) -> str:
        if self.duration <= 0:
            return "N/A"
        minutes, seconds = divmod(self.duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours > 0 else f"{minutes:02d}:{seconds:02d}"

    @property
    def is_search_query(self) -> bool:
        return self.original_id.startswith("ytmsearch")

    @property
    def ytm_search_query(self) -> str:
        query = f"{self.title} {self.artist}".strip()
        query = re.sub(r'[^\w\s-]', '', query)
        return f"ytmsearch1:{query}"

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
    id: str
    title: str
    artist: str
    uploader: str
    channel: str
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
    entries: List['YtDlpExtractedInfo']
    _type: str

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

class SpotifyToYTMAdapter:
    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self.client: Optional[spotipy.Spotify] = None
        
        client_id = os.environ.get("SPOTIPY_CLIENT_ID")
        client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
        
        if client_id and client_secret:
            auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
            self.client = spotipy.Spotify(auth_manager=auth_manager)
        else:
            self._logger.warning("[Auth] Credenciais do Spotify ausentes no ambiente. Instância operando em modo degradado.")

    def is_spotify_url(self, url: str) -> bool:
        return "open.spotify.com" in url

    def resolve(self, url: str) -> NormalizedMediaEntity:
        if not self.client:
            raise RuntimeError(
                "Motor de resolução Spotify inoperante: Credenciais OAuth2 não configuradas.\n\n"
                "Para restabelecer a comunicação DRM, injete as variáveis no ambiente de execução:\n\n"
                "[Windows PowerShell]:\n"
                "$env:SPOTIPY_CLIENT_ID=\"seu_client_id\"\n"
                "$env:SPOTIPY_CLIENT_SECRET=\"seu_client_secret\"\n\n"
                "[Unix / macOS / Bash]:\n"
                "export SPOTIPY_CLIENT_ID=\"seu_client_id\"\n"
                "export SPOTIPY_CLIENT_SECRET=\"seu_client_secret\"\n\n"
                "Reinicialize a aplicação após a alocação."
            )
            
        if "playlist" in url:
            return self._resolve_playlist(url)
        elif "track" in url:
            return self._resolve_track(url)
        elif "album" in url:
            return self._resolve_album(url)
        raise ValueError("Topologia Spotify não reconhecida (apenas Track, Album, Playlist).")

    def _resolve_track(self, url: str) -> NormalizedMediaEntity:
        track_id = self._extract_id(url, "track")
        track_data = self.client.track(track_id)
        return self._map_track_to_entity(track_data)

    def _resolve_playlist(self, url: str) -> NormalizedMediaEntity:
        playlist_id = self._extract_id(url, "playlist")
        playlist_info = self.client.playlist(playlist_id, fields='name,owner')
        results = self.client.playlist_items(playlist_id, additional_types=['track'])
        
        entities: List[NormalizedMediaEntity] = []
        for item in results.get('items', []):
            track = item.get('track')
            if track:
                entities.append(self._map_track_to_entity(track))
                
        return NormalizedMediaEntity(
            original_id=playlist_id,
            title=playlist_info.get('name', 'Unknown Playlist'),
            artist="Various Artists",
            album=playlist_info.get('name', 'Unknown Playlist'),
            is_playlist=True,
            children=entities
        )

    def _resolve_album(self, url: str) -> NormalizedMediaEntity:
        album_id = self._extract_id(url, "album")
        album_info = self.client.album(album_id)
        results = self.client.album_tracks(album_id)
        
        entities: List[NormalizedMediaEntity] = []
        for track in results.get('items', []):
            entities.append(self._map_track_to_entity(track, album_override=album_info.get('name', '')))
            
        return NormalizedMediaEntity(
            original_id=album_id,
            title=album_info.get('name', 'Unknown Album'),
            artist=album_info['artists'][0].get('name', 'Unknown') if album_info.get('artists') else 'Unknown',
            album=album_info.get('name', 'Unknown Album'),
            is_playlist=True,
            children=entities
        )

    def _map_track_to_entity(self, track_data: Dict[str, Any], album_override: str = "") -> NormalizedMediaEntity:
        artist = track_data['artists'][0].get('name', 'Unknown') if track_data.get('artists') else 'Unknown'
        album = album_override or track_data.get('album', {}).get('name', '')
        release_date = track_data.get('album', {}).get('release_date')
        
        thumb = None
        if track_data.get('album') and track_data['album'].get('images'):
            thumb = track_data['album']['images'][0].get('url')
            
        entity = NormalizedMediaEntity(
            original_id=track_data.get('id', ''),
            title=track_data.get('name', ''),
            artist=artist,
            album=album,
            duration=int(track_data.get('duration_ms', 0) / 1000),
            thumbnail_url=thumb,
            is_playlist=False,
            upload_date=release_date,
            description=None,
            channel=None
        )
        
        return NormalizedMediaEntity(
            original_id=entity.ytm_search_query,
            title=entity.title,
            artist=entity.artist,
            album=entity.album,
            duration=entity.duration,
            thumbnail_url=entity.thumbnail_url,
            is_playlist=False,
            upload_date=entity.upload_date,
            description=entity.description,
            channel=entity.channel
        )

    @staticmethod
    def _extract_id(url: str, entity_type: str) -> str:
        match = re.search(fr"spotify\.com/{entity_type}/([a-zA-Z0-9]+)", url)
        if not match:
            raise ValueError(f"URL corrompida: Falha na extração de hash de {entity_type}.")
        return match.group(1)

class YtDlpService:
    URL_REGEX = re.compile(
        r'^(https?://)?(www\.)?(youtube\.com|youtu\.be|music\.youtube\.com|vimeo\.com|soundcloud\.com|open\.spotify\.com)/.+$'
    )
    _network_semaphore = threading.BoundedSemaphore(value=5)

    @staticmethod
    def validate_url(url: str) -> bool:
        return bool(YtDlpService.URL_REGEX.match(url))

    @classmethod
    def extract_info_sync(cls, url: str) -> YtDlpExtractedInfo:
        logging.debug(f"[Network] Semáforo de rede adquirido: {url}")
        with cls._network_semaphore:
            ydl_opts: Dict[str, Any] = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': 'in_playlist',
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
                        "Interceção WAF (YouTube Anti-Bot).\n"
                        "Mitigação requerida: Injete cookies válidos ('cookies.txt') no diretório raiz."
                    )
                    logging.critical(f"[Security] WAF Triggered. {msg}")
                    raise RuntimeError(msg)
                raise

            return cast(YtDlpExtractedInfo, info)

    @classmethod
    def fetch_thumbnail_bytes_sync(cls, url: str) -> Optional[bytes]:
        try:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
                return response.read()
        except Exception as e:
            logging.error(f"[I/O] Falha na aquisição binária (Thumbnail): {e}")
            return None

class AnalysisWorker(QRunnable):
    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url
        self._signals = WorkerSignals()
        self.broker = PyQtMessageBroker(self._signals)
        self.spotify_adapter = SpotifyToYTMAdapter()

    @property
    def signals(self) -> WorkerSignals:
        return self._signals

    @pyqtSlot()
    def run(self) -> None:
        try:
            if self.spotify_adapter.is_spotify_url(self.url):
                entity = self.spotify_adapter.resolve(self.url)
            else:
                info = YtDlpService.extract_info_sync(self.url)
                entity = self._map_ytdlp_to_entity(info)

            self.broker.emit_result(entity)

            if entity.thumbnail_url and not entity.is_playlist:
                data = YtDlpService.fetch_thumbnail_bytes_sync(entity.thumbnail_url)
                if data:
                    self.broker.emit_thumbnail(data)
                    
        except Exception as e:
            logging.exception("Sub-rotina de análise interceptada com exceção crítica.")
            self.broker.emit_error(str(e))
        finally:
            self.broker.emit_finished()

    def _map_ytdlp_to_entity(self, info: YtDlpExtractedInfo) -> NormalizedMediaEntity:
        is_playlist = info.get('_type') == 'playlist' or 'entries' in info
        
        if is_playlist:
            children: List[NormalizedMediaEntity] = []
            for entry in info.get('entries', []):
                if entry:
                    children.append(self._map_ytdlp_to_entity(entry))
            
            return NormalizedMediaEntity(
                original_id=info.get('id', ''),
                title=info.get('title', 'Unknown Playlist'),
                artist=info.get('uploader', 'Unknown'),
                album=info.get('title', ''),
                is_playlist=True,
                children=children,
                upload_date=None,
                description=None,
                width=None,
                height=None,
                fps=None,
                channel=info.get('channel') or info.get('uploader')
            )
        else:
            return NormalizedMediaEntity(
                original_id=info.get('id', ''),
                title=info.get('title', 'Unknown'),
                artist=info.get('artist', info.get('uploader', 'Unknown')),
                album=info.get('album', ''),
                duration=info.get('duration', 0),
                thumbnail_url=info.get('thumbnail'),
                is_playlist=False,
                upload_date=info.get('upload_date'),
                description=info.get('description'),
                width=info.get('width'),
                height=info.get('height'),
                fps=info.get('fps'),
                channel=info.get('channel') or info.get('uploader')
            )

class PlaylistDispatcher:
    def __init__(self, thread_pool: QThreadPool) -> None:
        self.thread_pool = thread_pool
        self._logger = logging.getLogger(self.__class__.__name__)

    def orchestrate_download(self, root_entity: NormalizedMediaEntity, base_config: DownloadJobConfig) -> None:
        if not root_entity.is_playlist:
            self._dispatch_worker(root_entity, base_config)
            return

        children = root_entity.children or []
        self._logger.info(f"Topologia em Árvore detectada. Forking de {len(children)} workers concorrentes.")
        
        for child in children:
            self._dispatch_worker(child, base_config)

    def _dispatch_worker(self, entity: NormalizedMediaEntity, base_config: DownloadJobConfig) -> None:
        job_id = uuid.uuid4().hex[:12]
        target_url = entity.original_id if entity.is_search_query else f"https://www.youtube.com/watch?v={entity.original_id}"
        
        thread_safe_config = DownloadJobConfig(
            job_id=job_id,
            url=target_url,
            output_path=base_config.output_path,
            custom_filename=f"{entity.artist} - {entity.title}".replace("/", "_"),
            media_type=base_config.media_type,
            format_container=base_config.format_container,
            audio_codec=base_config.audio_codec,
            video_codec=base_config.video_codec,
            quality_preset=base_config.quality_preset,
            audio_sample_rate=base_config.audio_sample_rate,
            audio_bitrate=base_config.audio_bitrate,
            audio_bit_depth=base_config.audio_bit_depth,
            output_template=base_config.output_template,
            ffmpeg_path=base_config.ffmpeg_path,
            custom_flags=base_config.custom_flags,
            custom_cover_path=base_config.custom_cover_path,
            meta_title=entity.title,
            meta_artist=entity.artist,
            meta_album=entity.album,
            meta_date=entity.upload_date if entity.upload_date else base_config.meta_date,
            meta_desc=entity.description if entity.description else base_config.meta_desc,
            embed_metadata=base_config.embed_metadata,
            embed_thumbnail=base_config.embed_thumbnail,
            normalize_audio=base_config.normalize_audio
        )

        worker = DownloadWorker(thread_safe_config)
        self.thread_pool.start(worker)

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
            else: raise RuntimeError(f"Ficheiro de origem bloqueado ou inexistente: {raw_filepath}")

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
                logger.info("[DSP] Matriz gráfica injetada.")
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
            logger.error(f"Kernel Panic em Subprocesso DSP: {e.stderr}")
            raise RuntimeError(f"FFmpeg falhou ao processar matriz de áudio: {e.stderr}")
        finally:
            def safe_delete(p: Path) -> None:
                if not p or not p.exists(): return
                import time
                for _ in range(3):
                    try: p.unlink(); break
                    except PermissionError: time.sleep(1.0)
            
            if raw_filepath.exists() and raw_filepath.absolute() != out_filepath.absolute(): safe_delete(raw_filepath)
            if cover_target_path and "cover_art_fallback" in cover_target_path.name: safe_delete(cover_target_path)

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
        self._logger.warning("SIGTERM propagado em escopo local.")

    def _check_abort(self) -> None:
        if self._is_cancelled:
            raise DownloadError("Operação de I/O revogada pelo Scheduler.")

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
            self._logger.info("Ciclo de leitura binária saturado. Despachando transição de estado.")
            self.broker.emit_progress(self.config.job_id, 100.0, "Processing DSP")

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
            self._logger.error(f"Erro Léxico em Abstract Syntax Tree (Flags Customizadas): {e}")

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
                            self._logger.error(f"Falha de Concorrência ao relocar artefato (I/O Lock): {e}")
                            raise
                        import time
                        time.sleep(1.5)

        shutil.rmtree(self._temp_dir, ignore_errors=True)

    @pyqtSlot()
    def run(self) -> None:
        self.broker.emit_status(self.config.job_id, "Aquisição de Socket...")
        try:
            ydl_opts = self._build_ydl_opts()
            with YtDlpService._network_semaphore:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    self.broker.emit_status(self.config.job_id, "Iniciando descarga binária...")
                    info_dict = ydl.extract_info(self.config.url, download=True)
            
            self._check_abort()
            
            if self.config.media_type == MediaType.AUDIO:
                self.broker.emit_status(self.config.job_id, "Alocando Motor DSP...")
                raw_filepath = self._get_downloaded_filepath(cast(Dict[str, Any], info_dict))
                if raw_filepath:
                    SubprocessDSPEngine.execute_audio_pipeline(
                        self.config, raw_filepath, cast(Dict[str, Any], info_dict), self._temp_dir, self._logger
                    )

            self._finalize_move()
            self.broker.emit_status(self.config.job_id, "Hash Validado e Completo.")
            
        except Exception as e:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            if not self._is_cancelled:
                self._logger.error(f"Exceção não tratada em Kernel Scope: {e}", exc_info=True)
                self.broker.emit_error(str(e))
        finally:
            self.broker.emit_finished()