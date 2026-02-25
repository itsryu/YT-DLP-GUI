import logging
import shutil
import re
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Dict, Any

try:
    import yt_dlp # type: ignore
    from yt_dlp.utils import DownloadError # type: ignore
    import requests
except ImportError as e:
    raise ImportError(f"CRITICAL: Missing dependency. {e}. Please run 'pip install yt-dlp requests'")

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
    
    meta_title: str
    meta_artist: str
    meta_album: str
    meta_genre: str
    meta_date: str
    meta_desc: str
    
    embed_metadata: bool
    embed_thumbnail: bool
    embed_subs: bool
    normalize_audio: bool
    use_browser_cookies: bool

class WorkerSignals(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)
    progress = pyqtSignal(str, float, str)
    status = pyqtSignal(str, str)
    thumbnail_data = pyqtSignal(bytes)

class YtDlpInterceptorLogger:
    """
    Proxy Logger para interceptar o fluxo interno do yt-dlp.
    Permite o cancelamento imediato injetando uma exceção na thread assim que
    o yt-dlp tentar realizar qualquer log (ex: avisos de retry pós timeout).
    """
    def __init__(self, runnable):
        self.runnable = runnable
        self.logger = logging.getLogger(f"yt_dlp_{runnable.config.job_id[:8]}")

    def _check_abort(self):
        if self.runnable._is_cancelled:
            raise DownloadError("Operação cancelada pelo usuário")

    def debug(self, msg):
        self._check_abort()
        self.logger.debug(msg)

    def info(self, msg):
        self._check_abort()
        self.logger.info(msg)

    def warning(self, msg):
        self._check_abort()
        self.logger.warning(msg)

    def error(self, msg):
        self._check_abort()
        self.logger.error(msg)


class YtDlpService:
    URL_REGEX = re.compile(
        r'^(https?://)?(www\.)?(youtube\.com|youtu\.be|music\.youtube\.com|vimeo\.com|soundcloud\.com)/.+$'
    )

    @staticmethod
    def validate_url(url: str) -> bool:
        return bool(YtDlpService.URL_REGEX.match(url))

    @staticmethod
    def extract_info(url: str) -> Dict[str, Any]:
        logging.debug(f"Starting info extraction for: {url}")
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
            'socket_timeout': 10,
            'logger': logging.getLogger('yt_dlp_internal'),
            'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if 'entries' in info:
                logging.info("Playlist detected, using first entry.")
                info = info['entries'][0]
            return info

    @staticmethod
    def fetch_thumbnail_bytes(url: str) -> Optional[bytes]:
        try:
            with requests.get(url, stream=True, timeout=10) as r:
                r.raise_for_status()
                return r.content
        except Exception as e:
            logging.error(f"Thumbnail fetch failed: {e}")
            return None

class AnalysisRunnable(QRunnable):
    def __init__(self, url: str):
        super().__init__()
        self.url = url
        self.signals = WorkerSignals()

    @pyqtSlot()
    def run(self):
        try:
            info = YtDlpService.extract_info(self.url)
            
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
            
            self.signals.result.emit(meta)

            if meta.thumbnail_url:
                data = YtDlpService.fetch_thumbnail_bytes(meta.thumbnail_url)
                if data:
                    self.signals.thumbnail_data.emit(data)
                    
        except Exception as e:
            logging.exception("Analysis worker crashed.")
            self.signals.error.emit(str(e))
        finally:
            self.signals.finished.emit()

class DownloadRunnable(QRunnable):
    def __init__(self, config: DownloadJobConfig):
        super().__init__()
        self.config = config
        self.signals = WorkerSignals()
        self._is_cancelled = False
        self._temp_dir: Path = self.config.output_path / ".inprogress" / self.config.job_id
        self._logger = logging.getLogger(f"Job_{self.config.job_id[:8]}")

    def cancel(self):
        self._is_cancelled = True
        self._logger.warning("Cancel signal received.")

    def _progress_hook(self, d):
        if self._is_cancelled:
            raise DownloadError("Operation Cancelled by User")
        
        status = d.get('status')
        
        if status == 'downloading':
            p = d.get('_percent_str', '0%').replace('%', '')
            try: percent = float(p)
            except ValueError: percent = 0.0
            self.signals.progress.emit(self.config.job_id, percent, d.get('_speed_str', 'N/A'))
        
        elif status == 'finished':
            self._logger.info("Download finished. Injecting custom metadata into processing pipeline.")
            
            info_dict = d.get('info_dict')
            if info_dict and self.config.embed_metadata:
                if self.config.meta_title:
                    info_dict['title'] = self.config.meta_title
                if self.config.meta_artist:
                    info_dict['artist'] = self.config.meta_artist
                if self.config.meta_album:
                    info_dict['album'] = self.config.meta_album
                if self.config.meta_genre:
                    info_dict['genre'] = self.config.meta_genre
                if self.config.meta_desc:
                    info_dict['description'] = self.config.meta_desc
                    info_dict['comment'] = self.config.meta_desc
                if self.config.meta_date:
                    clean_date = self.config.meta_date.replace('-', '').replace('/', '')
                    if len(clean_date) == 4: clean_date += "0101"
                    info_dict['upload_date'] = clean_date
            
            self.signals.progress.emit(self.config.job_id, 100.0, "Processing")
            self.signals.status.emit(self.config.job_id, "Post-processing...")

    def _build_ydl_opts(self) -> Dict[str, Any]:
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        out_tmpl = str(self._temp_dir / f"{self.config.custom_filename}.%(ext)s")
        
        opts = {
            'outtmpl': out_tmpl,
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
            'retries': 10,
            'fragment_retries': 10,
            'socket_timeout': 10,
            'writethumbnail': self.config.embed_thumbnail,
            'addmetadata': self.config.embed_metadata,
            'writesubtitles': self.config.embed_subs,
            'logger': YtDlpInterceptorLogger(self),
            'postprocessors': [],
            'postprocessor_args': {},
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'web'],
                    'player_skip': ['web_safari', 'web_cabr']
                }
            }
        }

        if self.config.use_browser_cookies:
            self._logger.info("Using cookies from Chrome browser.")
            opts['cookiesfrombrowser'] = ('chrome', )

        if self.config.media_type == MediaType.AUDIO:
            opts['format'] = 'bestaudio/best'
            opts['postprocessors'].append({
                'key': 'FFmpegExtractAudio',
                'preferredcodec': self.config.format_container,
                'preferredquality': self.config.audio_bitrate if self.config.audio_bitrate != '0' else None,
            })
            if self.config.audio_sample_rate > 0:
                opts.setdefault('postprocessor_args', {}).setdefault('FFmpegExtractAudio', []).extend(
                    ['-ar', str(self.config.audio_sample_rate)]
                )
            if self.config.normalize_audio:
                opts.setdefault('postprocessor_args', {}).setdefault('FFmpegExtractAudio', []).extend(
                    ['-af', 'loudnorm=I=-14:TP=-1.5:LRA=11']
                )

        elif self.config.media_type == MediaType.VIDEO:
            res_map = {"4K": 2160, "2K": 1440, "1080p": 1080, "720p": 720, "480p": 480}
            target_h = res_map.get(self.config.quality_preset)
            res_filter = f"[height<={target_h}]" if target_h else ""
            
            v_fmt = f"bestvideo{res_filter}"
            if self.config.video_codec != 'best':
                v_codec_map = {'h264': 'avc', 'vp9': 'vp9', 'av1': 'av01'}
                vc = v_codec_map.get(self.config.video_codec, self.config.video_codec)
                v_fmt += f"[vcodec^={vc}]"
            
            a_fmt = "bestaudio"
            if self.config.audio_codec != 'best':
                a_fmt = f"bestaudio[acodec^={self.config.audio_codec}]"

            opts['format'] = f"{v_fmt}+{a_fmt}/best{res_filter}"
            opts['merge_output_format'] = self.config.format_container

        if self.config.embed_metadata:
            opts['postprocessors'].append({'key': 'FFmpegMetadata', 'add_chapters': True, 'add_metadata': True})

        if self.config.embed_thumbnail:
            opts['postprocessors'].append({'key': 'EmbedThumbnail'})

        return opts

    def _finalize_move(self):
        dest_dir = self.config.output_path
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        for file_p in self._temp_dir.iterdir():
            if file_p.is_file():
                target = dest_dir / file_p.name
                if target.exists():
                    self._logger.warning(f"Overwriting: {target}")
                    target.unlink()
                shutil.move(str(file_p), str(target))
        shutil.rmtree(self._temp_dir, ignore_errors=True)

    @pyqtSlot()
    def run(self):
        self.signals.status.emit(self.config.job_id, "Initializing...")
        try:
            ydl_opts = self._build_ydl_opts()
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                self.signals.status.emit(self.config.job_id, "Downloading...")
                ydl.download([self.config.url])
            
            if self._is_cancelled: 
                raise DownloadError("Cancelled")
            
            self._finalize_move()
            self.signals.status.emit(self.config.job_id, "Complete")
            self.signals.finished.emit()
            
        except Exception as e:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            if self._is_cancelled:
                pass
            else:
                self._logger.error(f"Failed: {e}", exc_info=True)
                self.signals.error.emit(str(e))