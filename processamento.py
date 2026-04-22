from __future__ import annotations

import collections
import dataclasses
import json
import logging
import os
import random
import re
import shlex
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.request
import uuid
import importlib.util
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import wraps
from pathlib import Path
from typing import (
    Any, Callable, Dict, Iterator, List, Optional, 
    Sequence, TypeVar, ClassVar, Final, cast
)

from PyQt6.QtCore import QObject, QRunnable, QThreadPool, pyqtSignal, pyqtSlot

# ============================================================================
# BOOTSTRAP & DEPENDÊNCIAS EXTERNAS
# ============================================================================
try:
    import yt_dlp
    from yt_dlp.utils import DownloadError
    from yt_dlp.networking.impersonate import ImpersonateTarget
except ImportError as exc:
    raise ImportError(f"CRITICAL: Dependência yt-dlp não resolvida. {exc}") from exc

_HAS_CURL_CFFI: Final[bool] = importlib.util.find_spec("curl_cffi") is not None
if not _HAS_CURL_CFFI:
    logging.getLogger(__name__).warning("[Dependência] 'curl_cffi' ausente. TLS Impersonation inativo.")

try:
    import spotipy  # type: ignore
    from spotipy.oauth2 import SpotifyClientCredentials  # type: ignore
except ImportError as exc:
    raise ImportError(f"CRITICAL: Dependência spotipy não resolvida. {exc}") from exc

# ============================================================================
# CAMADA DE DOMÍNIO (DOMAIN LAYER)
# ============================================================================
class MediaSystemError(Exception): pass
class NetworkError(MediaSystemError): pass
class NetworkBlockedCDNError(NetworkError): pass
class ExtractionError(MediaSystemError): pass
class ProcessingError(MediaSystemError): pass
class IOTimeoutError(MediaSystemError): pass

class MediaType(Enum):
    AUDIO = auto()
    VIDEO = auto()

@dataclass(frozen=True)
class AppConfig:
    network_timeout: int = 30
    recovery_timeout_cb: float = 45.0
    failure_threshold_cb: int = 4
    max_retries: int = 3
    base_backoff_delay: float = 2.0
    max_backoff_delay: float = 30.0

_APP_CONFIG: Final[AppConfig] = AppConfig()

@dataclass(frozen=True)
class NormalizedMediaEntity:
    original_id: str
    title: str
    artist: str
    album: str
    duration: float = 0.0  
    thumbnail_url: Optional[str] = None
    is_playlist: bool = False
    children: Optional[List['NormalizedMediaEntity']] = field(default_factory=list)
    upload_date: Optional[str] = None
    description: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[float] = None
    channel: Optional[str] = None
    filesize: int = 0

    @property
    def display_duration(self) -> str:
        safe_duration = int(float(self.duration)) if self.duration else 0
        if safe_duration <= 0: return "N/A"
        minutes, seconds = divmod(safe_duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours > 0 else f"{minutes:02d}:{seconds:02d}"

    @property
    def is_search_query(self) -> bool:
        return self.original_id.startswith(("ytmsearch", "ytsearch"))

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
    spotify_thumb_url: Optional[str] = None
    duration: float = 0.0

@dataclass
class DownloadJobState:
    custom_filename: str = ""
    custom_cover_path: Optional[str] = None
    resolved_output_path: Optional[str] = None
    ephemeral_cookie: Optional[str] = None

# ============================================================================
# CROSS-CUTTING CONCERNS (LOGGING, UTILS, RESILIÊNCIA)
# ============================================================================
class StructuredLogger:
    def __init__(self, name: str, context_id: str):
        self._logger = logging.getLogger(name)
        self._context_id = context_id

    def _format(self, msg: str, **kwargs: Any) -> str:
        payload = {"context": self._context_id, "msg": msg, **kwargs}
        return json.dumps(payload, ensure_ascii=False)

    def info(self, msg: str, **kwargs: Any) -> None: 
        if self._logger.isEnabledFor(logging.INFO): self._logger.info(self._format(msg, **kwargs))
    def warning(self, msg: str, **kwargs: Any) -> None: 
        if self._logger.isEnabledFor(logging.WARNING): self._logger.warning(self._format(msg, **kwargs))
    def error(self, msg: str, exc_info: bool = False, **kwargs: Any) -> None: 
        if self._logger.isEnabledFor(logging.ERROR): self._logger.error(self._format(msg, **kwargs), exc_info=exc_info)
    def debug(self, msg: str, **kwargs: Any) -> None: 
        if self._logger.isEnabledFor(logging.DEBUG): self._logger.debug(self._format(msg, **kwargs))

T = TypeVar('T')

def exponential_backoff(retries: int = 3, base_delay: float = 2.0, max_delay: float = 30.0) -> Callable:
    _RETRYABLE_PATTERNS = frozenset({"waf", "reload", "429", "rate limit", "timeout", "connection", "temporarily"})
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: Optional[Exception] = None
            sleep_prev = base_delay

            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    err_lower = str(exc).lower()
                    if not any(p in err_lower for p in _RETRYABLE_PATTERNS) or attempt == retries - 1:
                        raise NetworkError(f"Operação falhou irreversivelmente: {exc}") from exc
                    
                    last_exc = exc
                    delay = min(max_delay, random.uniform(base_delay, sleep_prev * 3))
                    sleep_prev = delay
                    logging.getLogger("NetworkBackoff").warning(f"Retentativa {attempt+1}/{retries} em {delay:.2f}s. Erro: {exc}")
                    time.sleep(delay)
            raise NetworkError(f"Exaustão de retentativas ({retries}).") from last_exc
        return wrapper
    return decorator

class CircuitBreakerState(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._lock = threading.Lock()

    def execute(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if time.monotonic() - self._last_failure_time > self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                else:
                    raise NetworkBlockedCDNError("CircuitBreaker: Intervenção em estado OPEN.")

        try:
            result = func(*args, **kwargs)
        except Exception:
            with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.monotonic()
                if self._failure_count >= self.failure_threshold or self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.OPEN
            raise

        with self._lock:
            if self._state in (CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED):
                self._state = CircuitBreakerState.CLOSED
                self._failure_count = 0
        return result

class SessionStateManager:
    @staticmethod
    def create_ephemeral_cookie_jar() -> Optional[str]:
        central_cookie = Path("cookies.txt")
        if not central_cookie.exists(): return None
        try:
            fd, temp_path = tempfile.mkstemp(suffix=".txt", prefix="sndstream_session_")
            os.close(fd)
            with open(central_cookie, 'rb') as src, open(temp_path, 'wb') as dst:
                shutil.copyfileobj(src, dst)
            return temp_path
        except Exception as e:
            logging.getLogger(__name__).error(f"[I/O] Falha na alocação de sandbox: {e}")
            return None

    @staticmethod
    def cleanup_ephemeral_cookie_jar(path: Optional[str]) -> None:
        if path and os.path.exists(path):
            try: os.unlink(path)
            except OSError: pass

# ============================================================================
# INFRAESTRUTURA E ADAPTERS
# ============================================================================
class MediaExtractorPort(ABC):
    @abstractmethod
    def resolve(self, url: str) -> NormalizedMediaEntity: pass

class AudioProcessorPort(ABC):
    @abstractmethod
    def execute_pipeline(self, config: DownloadJobConfig, state: DownloadJobState, raw_filepath: Path, temp_dir: Path, progress_cb: Callable[[float], None]) -> Path: pass

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

class IMediaBroker(IMessageBroker):
    @abstractmethod
    def emit_thumbnail(self, data: bytes) -> None: pass

class SpotifyAdapter(MediaExtractorPort):
    def __init__(self) -> None:
        self._logger = logging.getLogger(self.__class__.__name__)
        self.client: Optional[spotipy.Spotify] = None
        client_id, client_secret = os.environ.get("SPOTIPY_CLIENT_ID"), os.environ.get("SPOTIPY_CLIENT_SECRET")
        if client_id and client_secret:
            self.client = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

    def is_spotify_url(self, url: str) -> bool:
        return "open.spotify.com" in url

    def resolve(self, url: str) -> NormalizedMediaEntity:
        if not self.client: raise ExtractionError("Motor Spotify inoperante: Credenciais ausentes.")
        try:
            if "playlist" in url: return self._resolve_playlist(url)
            elif "track" in url: return self._resolve_track(url)
            elif "album" in url: return self._resolve_album(url)
            raise ValueError("URI Spotify não reconhecida.")
        except Exception as e:
            raise ExtractionError(f"Falha na resolução Spotify: {e}") from e

    def _paginate_results(self, initial_page: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        results = initial_page
        while results:
            yield from results.get('items', [])
            if not results.get('next'): break
            try:
                results = self.client.next(results)  # type: ignore
            except spotipy.SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(getattr(e, 'headers', {}).get('Retry-After', 5))
                    time.sleep(retry_after)
                    results = self.client.next(results)  # type: ignore
                else: raise

    def _resolve_track(self, url: str) -> NormalizedMediaEntity:
        return self._map_track_to_entity(self.client.track(self._extract_id(url, "track"))) # type: ignore

    def _resolve_playlist(self, url: str) -> NormalizedMediaEntity:
        playlist_id = self._extract_id(url, "playlist")
        playlist_info = self.client.playlist(playlist_id, fields='name,owner') # type: ignore
        entities = [self._map_track_to_entity(item['track']) for item in self._paginate_results(self.client.playlist_items(playlist_id, additional_types=['track'])) if item.get('track')] # type: ignore
        return NormalizedMediaEntity(
            original_id=playlist_id, title=playlist_info.get('name', 'Unknown Playlist'), artist="Various Artists",
            album=playlist_info.get('name', 'Unknown Playlist'), is_playlist=True, children=entities
        )

    def _resolve_album(self, url: str) -> NormalizedMediaEntity:
        album_id = self._extract_id(url, "album")
        album_info = self.client.album(album_id) # type: ignore
        album_name = album_info.get('name', 'Unknown Album')
        artists_data = album_info.get('artists', [])
        album_artist = ", ".join([a.get('name', 'Unknown') for a in artists_data]) if artists_data else 'Unknown'
        entities = [self._map_track_to_entity(track, album_override=album_name) for track in self._paginate_results(self.client.album_tracks(album_id))] # type: ignore
        return NormalizedMediaEntity(
            original_id=album_id, title=album_name, artist=album_artist, album=album_name,
            is_playlist=True, children=entities
        )

    def _map_track_to_entity(self, track_data: Dict[str, Any], album_override: str = "") -> NormalizedMediaEntity:
        artists_data = track_data.get('artists') or []
        artist = ", ".join([a.get('name', 'Unknown') for a in artists_data]) if artists_data else 'Unknown'
        album_data = track_data.get('album') or {}
        album = album_override or album_data.get('name', '')
        images = album_data.get('images') or []
        thumb = images[0].get('url') if isinstance(images, list) and len(images) > 0 else None
        
        entity = NormalizedMediaEntity(
            original_id=track_data.get('id', ''), title=track_data.get('name', 'Unknown Track'),
            artist=artist, album=album, duration=float(track_data.get('duration_ms', 0) / 1000.0),
            thumbnail_url=thumb, upload_date=album_data.get('release_date')
        )
        return dataclasses.replace(entity, original_id=entity.ytm_search_query)

    @staticmethod
    def _extract_id(url: str, entity_type: str) -> str:
        match = re.search(fr"/{entity_type}/([a-zA-Z0-9]+)", url)
        if not match: raise ValueError(f"Falha léxica ao extrair hash de {entity_type}.")
        return match.group(1)

class YtDlpAdapter(MediaExtractorPort):
    _network_semaphore = threading.BoundedSemaphore(value=5)
    _circuit_breaker: CircuitBreaker = CircuitBreaker(_APP_CONFIG.failure_threshold_cb, _APP_CONFIG.recovery_timeout_cb)

    @exponential_backoff(retries=3)
    def resolve(self, url: str) -> NormalizedMediaEntity:
        opts = {
            'quiet': True, 'no_warnings': True, 'extract_flat': 'in_playlist',
            'socket_timeout': _APP_CONFIG.network_timeout, 'source_address': '0.0.0.0',
            'logger': logging.getLogger('yt_dlp_adapter'), 'javascript_executor': 'deno', 'cachedir': False
        }
        target = self._get_tls_target()
        if target: opts['impersonate'] = target

        def _extract() -> Dict[str, Any]:
            with self._network_semaphore:
                try:
                    with yt_dlp.YoutubeDL(opts) as ydl:
                        return cast(Dict[str, Any], ydl.extract_info(url, download=False))
                except DownloadError as e:
                    if any(kw in str(e).lower() for kw in ["sign in", "members only", "private", "age"]):
                        return self._fallback_extract_with_cookies(url, opts)
                    raise ExtractionError(str(e)) from e
        
        raw_info = self._circuit_breaker.execute(_extract)
        return self._map_to_entity(raw_info)

    def _fallback_extract_with_cookies(self, url: str, base_opts: dict) -> Dict[str, Any]:
        ephemeral_cookie = SessionStateManager.create_ephemeral_cookie_jar()
        if not ephemeral_cookie: raise ExtractionError("Falha na geração de sandbox de cookies para fallback.")
        opts = base_opts.copy()
        opts['cookiefile'] = ephemeral_cookie
        try:
            with yt_dlp.YoutubeDL(opts) as ydl:
                return cast(Dict[str, Any], ydl.extract_info(url, download=False))
        finally:
            SessionStateManager.cleanup_ephemeral_cookie_jar(ephemeral_cookie)

    def _map_to_entity(self, info: Dict[str, Any]) -> NormalizedMediaEntity:
        is_playlist = info.get('_type') == 'playlist' or 'entries' in info
        canonical_id = info.get('webpage_url') or info.get('url') or info.get('id', '')
        
        if is_playlist:
            return NormalizedMediaEntity(
                original_id=canonical_id, title=info.get('title', 'Unknown Playlist'), artist=info.get('uploader', 'Unknown'),
                album=info.get('title', ''), is_playlist=True, channel=info.get('channel') or info.get('uploader'),
                children=[self._map_to_entity(entry) for entry in info.get('entries', []) if entry]
            )
        
        return NormalizedMediaEntity(
            original_id=canonical_id, title=info.get('title', 'Unknown'), artist=info.get('artist', info.get('uploader', 'Unknown')),
            album=info.get('album', ''), duration=float(info.get('duration', 0.0) or 0.0), thumbnail_url=info.get('thumbnail'),
            is_playlist=False, upload_date=info.get('upload_date'), description=info.get('description'),
            width=info.get('width'), height=info.get('height'), fps=info.get('fps'), channel=info.get('channel') or info.get('uploader'),
            filesize=info.get('filesize_approx') or info.get('filesize') or 0
        )

    @classmethod
    def fetch_thumbnail(cls, url: str) -> Optional[bytes]:
        try:
            ctx = ssl.create_default_context()
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
                return bytes(resp.read())
        except Exception:
            return None

    @staticmethod
    def _get_tls_target() -> Optional[ImpersonateTarget]:
        if not _HAS_CURL_CFFI: return None
        try:
            with yt_dlp.YoutubeDL({"quiet": True}) as ydl:
                if ydl._impersonate_target_available(ImpersonateTarget(client="chrome110")):
                    return ImpersonateTarget(client="chrome110")
        except Exception: pass
        return None

class FFmpegCommandBuilder:
    _PCM_MAP: Final[Dict[str, str]] = {"16": "pcm_s16le", "24": "pcm_s24le", "32": "pcm_s32le"}

    def __init__(self, config: DownloadJobConfig, source_path: Path, dest_path: Path, cover_path: Optional[Path]):
        self.config = config
        self.source_path = source_path
        self.dest_path = dest_path
        self.cover_path = cover_path
        self._cmd: List[str] = [config.ffmpeg_path or 'ffmpeg', '-y', '-i', str(source_path)]

    def build(self) -> List[str]:
        self._apply_inputs()
        self._apply_audio_codecs()
        self._apply_audio_filters()
        self._apply_metadata()
        self._apply_mapping_and_output()
        return self._cmd

    def _apply_inputs(self) -> None:
        if self.cover_path: self._cmd.extend(['-i', str(self.cover_path)])

    def _apply_audio_codecs(self) -> None:
        ext = self.config.format_container
        bit_depth = str(self.config.audio_bit_depth)
        
        if ext == 'flac': 
            self._cmd.extend(['-c:a', 'flac'])
            if bit_depth not in ("auto", "16"):
                # Mapeamento do alinhamento de memória (24-bit no FFmpeg exige buffer s32)
                fmt = 's32' if bit_depth in ("24", "32") else f's{bit_depth}'
                self._cmd.extend(['-sample_fmt', fmt])
        elif ext == 'wav': 
            codec = self._PCM_MAP.get(bit_depth, 'pcm_s16le')
            self._cmd.extend(['-c:a', codec]) 
        elif ext in ['mp3', 'm4a', 'aac']:
            self._cmd.extend(['-c:a', 'aac' if ext in ['m4a', 'aac'] else 'libmp3lame'])
            if self.config.audio_bitrate != '0': self._cmd.extend(['-b:a', f"{self.config.audio_bitrate}k"])

    def _apply_audio_filters(self) -> None:
        filters, aresample_opts = [], []
        bit_depth = str(self.config.audio_bit_depth)
        
        if self.config.audio_sample_rate > 0: aresample_opts.append(f"osr={self.config.audio_sample_rate}")
        if self.config.format_container in ['flac', 'wav'] and bit_depth != 'auto':
            # FFmpeg AVSampleFormat mapeia 24-bit físico para buffer lógico 's32'
            osf_fmt = 's32' if bit_depth in ("24", "32") else 's16'
            aresample_opts.extend([f"osf={osf_fmt}", "dither_method=triangular"])
        if aresample_opts: filters.append(f"aresample={':'.join(aresample_opts)}")
        if self.config.normalize_audio: filters.append("loudnorm=I=-14:TP=-1.5:LRA=11")
        if filters: self._cmd.extend(['-af', ','.join(filters)])

    def _apply_metadata(self) -> None:
        if not self.config.embed_metadata: return
        meta_dict = {
            'title': self.config.meta_title, 'artist': self.config.meta_artist,
            'album_artist': self.config.meta_artist, 'album': self.config.meta_album,
            'genre': self.config.meta_genre, 'comment': self.config.meta_desc
        }
        for k, v in meta_dict.items():
            if v: self._cmd.extend(['-metadata', f"{k}={v}"])
        if self.config.meta_date:
            self._cmd.extend(['-metadata', f"date={str(self.config.meta_date[:4]).strip()}"])

    def _apply_mapping_and_output(self) -> None:
        if self.cover_path:
            self._cmd.extend(['-map', '0:a:0', '-map', '1:v:0', '-c:v', 'mjpeg', '-disposition:v', 'attached_pic'])
            if self.config.format_container == 'mp3':
                self._cmd.extend(['-id3v2_version', '3', '-metadata:s:v', 'title=Album cover', '-metadata:s:v', 'comment=Cover (front)'])
        else: self._cmd.extend(['-map', '0:a:0'])
        self._cmd.append(str(self.dest_path))

class FFmpegAdapter(AudioProcessorPort):
    def execute_pipeline(self, config: DownloadJobConfig, state: DownloadJobState, raw_filepath: Path, temp_dir: Path, progress_cb: Callable[[float], None]) -> Path:
        out_filepath = raw_filepath.with_suffix(f".{config.format_container}")
        if raw_filepath.absolute() == out_filepath.absolute():
            new_raw = raw_filepath.with_suffix(".raw_audio")
            shutil.move(str(raw_filepath), str(new_raw))
            raw_filepath = new_raw

        cover_path = Path(state.custom_cover_path) if state.custom_cover_path and Path(state.custom_cover_path).exists() else None
        cmd_builder = FFmpegCommandBuilder(config, raw_filepath, out_filepath, cover_path)
        cmd_progress = cmd_builder.build()
        cmd_progress.insert(-1, '-progress')
        cmd_progress.insert(-1, 'pipe:1')
        cmd_progress.insert(-1, '-nostats')

        timeout_val = max(120, int((config.duration or 300) * 3))
        
        try:
            self._run_process(cmd_progress, timeout_val, progress_cb, float(config.duration or 300.0))
            return out_filepath
        except subprocess.CalledProcessError as e:
            raise ProcessingError(f"Falha de Processamento FFmpeg: {e.stderr}") from e
        except subprocess.TimeoutExpired as e:
            raise IOTimeoutError("Motor DSP excedeu timeout de execução estipulado.") from e
        finally:
            self._cleanup_intermediates(raw_filepath, out_filepath, cover_path)

    def _run_process(self, cmd: List[str], timeout: int, progress_cb: Callable[[float], None], real_duration: float) -> None:
        startupinfo = subprocess.STARTUPINFO() if os.name == 'nt' else None
        if startupinfo: startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        
        error_log = collections.deque(maxlen=50)
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding='utf-8', errors='replace', startupinfo=startupinfo
        ) as proc:
            assert proc.stdout is not None
            for line in proc.stdout:
                if line.startswith('out_time_ms='):
                    try:
                        elapsed_ms = int(line.split('=')[1])
                        pct = min(100.0, (elapsed_ms / 1_000_000) / max(real_duration, 1.0) * 100)
                        progress_cb(pct)
                    except ValueError: pass
                elif line.strip(): error_log.append(line.strip())
            
            proc.wait(timeout=timeout)
            if proc.returncode != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd, stderr='\n'.join(error_log))

    def _cleanup_intermediates(self, raw_path: Path, out_path: Path, cover_path: Optional[Path]) -> None:
        def safe_delete(p: Path) -> None:
            if not p or not p.exists(): return
            for _ in range(3):
                try: p.unlink(); break
                except PermissionError: time.sleep(0.5)
        if raw_path.exists() and raw_path.absolute() != out_path.absolute(): safe_delete(raw_path)
        if cover_path and "sp_cover_" in cover_path.name: safe_delete(cover_path)

# ============================================================================
# USE CASES & WORKERS
# ============================================================================
class PyQtMessageBroker(IMediaBroker):
    def __init__(self, signals: WorkerSignals): self.signals = signals
    def emit_finished(self) -> None: self.signals.finished.emit()
    def emit_error(self, message: str) -> None: self.signals.error.emit(message)
    def emit_result(self, result: Any) -> None: self.signals.result.emit(result)
    def emit_progress(self, job_id: str, percent: float, speed: str) -> None: self.signals.progress.emit(job_id, percent, speed)
    def emit_status(self, job_id: str, status: str) -> None: self.signals.status.emit(job_id, status)
    def emit_thumbnail(self, data: bytes) -> None: self.signals.thumbnail_data.emit(data)

class WorkerSignals(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)
    progress = pyqtSignal(str, float, str)
    status = pyqtSignal(str, str)
    thumbnail_data = pyqtSignal(bytes)

class AnalysisWorker(QRunnable):
    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url
        self._signals = WorkerSignals()
        self.broker = PyQtMessageBroker(self._signals)
        self.spotify_adapter = SpotifyAdapter()
        self.ytdlp_adapter = YtDlpAdapter()

    @property
    def signals(self) -> WorkerSignals: return self._signals

    @pyqtSlot()
    def run(self) -> None:
        try:
            if self.spotify_adapter.is_spotify_url(self.url):
                entity = self.spotify_adapter.resolve(self.url)
            else:
                entity = self.ytdlp_adapter.resolve(self.url)
            self.broker.emit_result(entity)
            
            if entity.thumbnail_url and not entity.is_playlist:
                data = YtDlpAdapter.fetch_thumbnail(entity.thumbnail_url)
                if data: self.broker.emit_thumbnail(data)
        except MediaSystemError as e:
            self.broker.emit_error(str(e))
        except Exception as e:
            self.broker.emit_error(f"Erro Crítico de Sistema: {str(e)}")
        finally:
            self.broker.emit_finished()

class WorkspaceManager:
    def __init__(self, base_dir: Path, job_id: str):
        self.temp_dir = base_dir / ".inprogress" / job_id

    def setup(self) -> Path:
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        if os.name == 'nt':
            try:
                import ctypes
                ctypes.windll.kernel32.SetFileAttributesW(str(self.temp_dir.parent), 0x02)
            except Exception: pass
        return self.temp_dir

    def teardown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        try: self.temp_dir.parent.rmdir()
        except OSError: pass

class CoverArtResolver:
    @staticmethod
    def resolve(config: DownloadJobConfig, state: DownloadJobState) -> None:
        if not config.spotify_thumb_url or not config.embed_thumbnail: return
        if state.custom_cover_path and Path(state.custom_cover_path).exists(): return
        raw: Optional[bytes] = YtDlpAdapter.fetch_thumbnail(config.spotify_thumb_url)
        if raw:
            fd, path = tempfile.mkstemp(suffix=".jpg", prefix="sp_cover_")
            os.close(fd)
            with open(path, "wb") as f:
                f.write(raw)
            state.custom_cover_path = path

class DownloadWorker(QRunnable):
    def __init__(self, config: DownloadJobConfig, app_config: AppConfig = _APP_CONFIG) -> None:
        super().__init__()
        self.config = config
        self._app_config = app_config
        self.state = DownloadJobState()
        self._signals = WorkerSignals()
        self.broker = PyQtMessageBroker(self._signals)
        self._abort_event = threading.Event()
        self._workspace = WorkspaceManager(self.config.output_path, self.config.job_id)
        self._logger = StructuredLogger(__name__, self.config.job_id[:8])

    @property
    def signals(self) -> WorkerSignals: return self._signals

    def cancel(self) -> None: self._abort_event.set()
    def _check_abort(self) -> None:
        if self._abort_event.is_set(): raise ExtractionError("Operação I/O revogada via IPC.")

    @pyqtSlot()
    def run(self) -> None:
        temp_dir = self._workspace.setup()
        try:
            self.broker.emit_status(self.config.job_id, "Iniciando orquestração...")
            CoverArtResolver.resolve(self.config, self.state)
            
            opts = self._build_opts(temp_dir)
            raw_filepath = self._execute_download(opts, temp_dir)
            
            if self.config.media_type == MediaType.AUDIO and raw_filepath:
                self.broker.emit_status(self.config.job_id, "Motor DSP acionado.")
                processor = FFmpegAdapter()
                processor.execute_pipeline(
                    self.config, self.state, raw_filepath, temp_dir,
                    lambda pct: self.broker.emit_progress(self.config.job_id, pct, "Processamento DSP")
                )

            self._atomic_finalize_move(temp_dir)
            self.broker.emit_status(self.config.job_id, "Pipeline concluído com sucesso.")

        except MediaSystemError as e:
            if not self._abort_event.is_set():
                self._logger.error("Falha no Domínio MediaSystem", exc_info=True)
                self.broker.emit_error(str(e))
        except Exception as e:
            if not self._abort_event.is_set():
                self._logger.error("Falha não tratada", exc_info=True)
                self.broker.emit_error(f"Erro Crítico: {e}")
        finally:
            SessionStateManager.cleanup_ephemeral_cookie_jar(self.state.ephemeral_cookie)
            self._workspace.teardown()
            self.broker.emit_finished()

    def _progress_hook(self, d: dict[str, Any]) -> None:
        self._check_abort()
        status = d.get('status')
        if status == 'downloading':
            p = d.get('_percent_str', '0%').replace('%', '')
            try: percent = float(p)
            except ValueError: percent = 0.0
            self.broker.emit_progress(self.config.job_id, percent, d.get('_speed_str', 'N/A'))
        elif status == 'finished':
            self.broker.emit_progress(self.config.job_id, 100.0, "Escrita final...")

    def _build_opts(self, temp_dir: Path) -> Dict[str, Any]:
        out_tmpl = str(temp_dir / f"%(title)s - %(uploader)s [%(id)s].%(ext)s")
        opts: Dict[str, Any] = {
            'outtmpl': out_tmpl, 'progress_hooks': [self._progress_hook],
            'quiet': True, 'no_warnings': False, 'socket_timeout': self._app_config.network_timeout,
            'source_address': '0.0.0.0', 'javascript_executor': 'deno',
            'format': 'bestaudio/best' if self.config.media_type == MediaType.AUDIO else 'bestvideo+bestaudio/best',
            'merge_output_format': self.config.format_container if self.config.media_type == MediaType.VIDEO else None
        }
        if self.config.ffmpeg_path: opts['ffmpeg_location'] = self.config.ffmpeg_path
        if self.config.use_browser_cookies:
            self.state.ephemeral_cookie = SessionStateManager.create_ephemeral_cookie_jar()
            if self.state.ephemeral_cookie: opts['cookiefile'] = self.state.ephemeral_cookie

        if self.config.custom_flags:
            try: extra_tokens = shlex.split(self.config.custom_flags)
            except ValueError: extra_tokens = self.config.custom_flags.split()
            
            _FLAG_MAP: Final = {
                "--force-ipv4": {"source_address": "0.0.0.0"},
                "--force-ipv6": {"source_address": "::"},
                "--geo-bypass": {"geo_bypass": True},
                "--no-overwrites": {"nooverwrites": True},
                "--ignore-errors": {"ignoreerrors": True},
                "--no-warnings": {"no_warnings": True},
                "--restrict-filenames": {"restrictfilenames": True},
                "--windows-filenames": {"windowsfilenames": True},
                "--continue": {"continuedl": True},
                "--write-subs": {"writesubtitles": True},
                "--write-auto-subs": {"writeautomaticsub": True},
                "--write-info-json": {"writeinfojson": True}
            }
            i = 0
            while i < len(extra_tokens):
                tok = extra_tokens[i]
                if tok in _FLAG_MAP:
                    opts.update(_FLAG_MAP[tok])
                elif tok == "--proxy" and i + 1 < len(extra_tokens):
                    opts["proxy"] = extra_tokens[i + 1]; i += 1
                elif tok == "--limit-rate" and i + 1 < len(extra_tokens):
                    opts["ratelimit"] = extra_tokens[i + 1]; i += 1
                elif tok == "--socket-timeout" and i + 1 < len(extra_tokens):
                    opts["socket_timeout"] = float(extra_tokens[i + 1]); i += 1
                elif tok == "--match-filter" and i + 1 < len(extra_tokens):
                    from yt_dlp.utils import match_filter_func
                    opts["match_filter"] = match_filter_func(extra_tokens[i + 1]); i += 1
                elif tok == "--extractor-args" and i + 1 < len(extra_tokens):
                    val = extra_tokens[i + 1]
                    if ':' in val and '=' in val:
                        extractor, rest = val.split(':', 1)
                        arg_k, arg_v = rest.split('=', 1)
                        opts.setdefault('extractor_args', {}).setdefault(extractor, {}).setdefault(arg_k, []).append(arg_v)
                    i += 1
                i += 1
        return opts

    def _execute_download(self, opts: Dict[str, Any], temp_dir: Path) -> Optional[Path]:
        def _dl() -> Dict[str, Any]:
            with YtDlpAdapter._network_semaphore:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    return cast(Dict[str, Any], ydl.extract_info(self.config.url, download=True))
        
        info = YtDlpAdapter._circuit_breaker.execute(_dl)
        self._check_abort()
        
        filepath = info.get('filepath') or info.get('_filename')
        if filepath: return Path(filepath)
        files = [f for f in temp_dir.iterdir() if f.is_file() and not f.name.endswith(('.part', '.ytdl'))]
        if files: return sorted(files, key=lambda x: x.stat().st_size, reverse=True)[0]
        return None

    def _atomic_finalize_move(self, temp_dir: Path) -> None:
        dest_dir = self.config.output_path
        dest_dir.mkdir(parents=True, exist_ok=True)
        moved_files = []
        for file_p in temp_dir.iterdir():
            if not file_p.is_file() or file_p.suffix.lower() in {".part", ".ytdl"}: continue
            
            counter = 0
            while True:
                name = f"{self.state.custom_filename}{f' ({counter})' if counter else ''}{file_p.suffix}"
                target = dest_dir / name
                try:
                    fd = os.open(str(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
                    os.close(fd)
                    target.unlink()
                    shutil.move(str(file_p), str(target))
                    moved_files.append(target)
                    break
                except FileExistsError:
                    counter += 1
            
        if moved_files:
            target_out = next((f for f in moved_files if f.suffix.lower() == f".{self.config.format_container.lower()}"), moved_files[0])
            self.state.resolved_output_path = str(target_out)
            self.state.custom_filename = target_out.stem

class PlaylistDispatcher:
    def __init__(self, thread_pool: QThreadPool) -> None:
        self.thread_pool = thread_pool

    def orchestrate_download(self, root_entity: NormalizedMediaEntity, base_config: DownloadJobConfig) -> None:
        if not root_entity.is_playlist:
            self._dispatch_worker(root_entity, base_config)
            return
        for child in (root_entity.children or []):
            self._dispatch_worker(child, base_config)

    def _dispatch_worker(self, entity: NormalizedMediaEntity, base_config: DownloadJobConfig) -> None:
        thread_safe_config = dataclasses.replace(
            base_config,
            job_id=uuid.uuid4().hex[:12],
            url=entity.original_id if entity.is_search_query else f"https://www.youtube.com/watch?v={entity.original_id}",
            meta_title=entity.title, meta_artist=entity.artist, meta_album=entity.album,
            meta_date=entity.upload_date if entity.upload_date else base_config.meta_date,
            meta_desc=entity.description if entity.description else base_config.meta_desc,
            duration=entity.duration
        )
        self.thread_pool.start(DownloadWorker(thread_safe_config))