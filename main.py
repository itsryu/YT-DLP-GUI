import sys
import logging
import shutil
import uuid
import re
from dataclasses import dataclass
from enum import Enum, auto
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Dict, Any, List, Final

try:
    import yt_dlp # type: ignore
    from yt_dlp.utils import DownloadError # type: ignore
    import requests
except ImportError as e:
    print(f"CRITICAL: Missing dependency. {e}")
    sys.exit(1)

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QAbstractItemView, QMessageBox, QFrame, QScrollArea,
    QGroupBox, QFormLayout, QCheckBox, QSpinBox, QStyleFactory, QPlainTextEdit,
    QSplitter, QTabWidget, QRadioButton, QButtonGroup
)
from PyQt6.QtCore import (
    Qt, QObject, pyqtSignal, QRunnable, QThreadPool, pyqtSlot
)
from PyQt6.QtGui import QColor, QPixmap, QFont, QTextCursor, QTextCharFormat

APP_NAME: Final[str] = "SoundStream Pro"
VERSION: Final[str] = "7.1.0"
DEFAULT_DOWNLOAD_DIR: Final[Path] = Path.home() / "Downloads"
LOG_FILENAME: Final[str] = "soundstream_audit.log"
MAX_CONCURRENT_DOWNLOADS: Final[int] = 3

class QtLogHandler(logging.Handler, QObject):
    log_record = pyqtSignal(str, int)

    def __init__(self):
        logging.Handler.__init__(self)
        QObject.__init__(self)
        self.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record: logging.LogRecord):
        msg = self.format(record)
        self.log_record.emit(msg, record.levelno)

class LogViewerWidget(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._init_ui()
        self._setup_styles()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        header_layout = QHBoxLayout()
        lbl = QLabel("SYSTEM LOGS / DEBUG CONSOLE")
        lbl.setStyleSheet("color: #666; font-weight: bold; font-size: 10px; letter-spacing: 1px;")
        
        btn_clear = QPushButton("Clear")
        btn_clear.setFixedSize(60, 20)
        btn_clear.setStyleSheet("background-color: #333; color: #aaa; border: none; font-size: 10px;")
        btn_clear.clicked.connect(self.clear_logs)
        
        header_layout.addWidget(lbl)
        header_layout.addStretch()
        header_layout.addWidget(btn_clear)
        
        self.text_edit = QPlainTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setStyleSheet("""
            QPlainTextEdit {
                background-color: #0e0e0e; 
                color: #d4d4d4; 
                font-family: 'Consolas', 'Courier New', monospace; 
                font-size: 11px;
                border: 1px solid #333;
                border-radius: 4px;
            }
        """)
        self.text_edit.setMaximumBlockCount(2000) 
        
        layout.addLayout(header_layout)
        layout.addWidget(self.text_edit)

    def _setup_styles(self):
        self.fmt_debug = QTextCharFormat()
        self.fmt_debug.setForeground(QColor("#808080"))
        self.fmt_info = QTextCharFormat()
        self.fmt_info.setForeground(QColor("#569cd6"))
        self.fmt_warning = QTextCharFormat()
        self.fmt_warning.setForeground(QColor("#dcdcaa"))
        self.fmt_error = QTextCharFormat()
        self.fmt_error.setForeground(QColor("#f44747"))
        self.fmt_critical = QTextCharFormat()
        self.fmt_critical.setForeground(QColor("#ff0000"))
        self.fmt_critical.setFontWeight(QFont.Weight.Bold)

    @pyqtSlot(str, int)
    def append_log(self, msg: str, levelno: int):
        cursor = self.text_edit.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        if levelno >= logging.CRITICAL: cursor.setCharFormat(self.fmt_critical)
        elif levelno >= logging.ERROR: cursor.setCharFormat(self.fmt_error)
        elif levelno >= logging.WARNING: cursor.setCharFormat(self.fmt_warning)
        elif levelno == logging.INFO: cursor.setCharFormat(self.fmt_info)
        else: cursor.setCharFormat(self.fmt_debug)
        cursor.insertText(msg + "\n")
        self.text_edit.setTextCursor(cursor)
        self.text_edit.ensureCursorVisible()

    def clear_logs(self):
        self.text_edit.clear()

# --- DATA MODELS ---
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

# --- WORKER SIGNALS ---
class WorkerSignals(QObject):
    finished = pyqtSignal()
    error = pyqtSignal(str)
    result = pyqtSignal(object)
    progress = pyqtSignal(str, float, str)
    status = pyqtSignal(str, str)
    thumbnail_data = pyqtSignal(bytes)

# --- BUSINESS LOGIC SERVICES ---
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

# --- WORKERS ---
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
        """
        Intervention Hook:
        Modifies the info_dict in-memory immediately after download completes
        but BEFORE post-processing (FFmpeg) begins. This ensures tags are written correctly.
        """
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
            'socket_timeout': 30,
            'writethumbnail': self.config.embed_thumbnail,
            'addmetadata': self.config.embed_metadata,
            'writesubtitles': self.config.embed_subs,
            'logger': logging.getLogger(f"yt_dlp_{self.config.job_id[:8]}"),
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
            
            if self._is_cancelled: raise DownloadError("Cancelled")
            self._finalize_move()
            self.signals.status.emit(self.config.job_id, "Complete")
            self.signals.finished.emit()
        except Exception as e:
            shutil.rmtree(self._temp_dir, ignore_errors=True)
            if self._is_cancelled:
                self.signals.status.emit(self.config.job_id, "Cancelled")
            else:
                self._logger.error(f"Failed: {e}", exc_info=True)
                self.signals.error.emit(str(e))

# --- UI COMPONENTS ---
class ModernStyle:
    STYLESHEET: Final[str] = """
    QMainWindow { background-color: #121212; }
    QWidget { color: #e0e0e0; font-family: 'Segoe UI', 'Roboto', sans-serif; font-size: 13px; }
    QFrame#Panel { background-color: #1e1e1e; border-radius: 8px; border: 1px solid #333; }
    QGroupBox { border: 1px solid #3d3d3d; border-radius: 6px; margin-top: 20px; font-weight: bold; color: #007acc; }
    QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; }
    QLineEdit, QComboBox, QSpinBox, QPlainTextEdit { background-color: #252526; border: 1px solid #3d3d3d; border-radius: 4px; padding: 6px; color: white; selection-background-color: #007acc; }
    QLineEdit:focus, QComboBox:focus, QSpinBox:focus { border: 1px solid #007acc; }
    QPushButton { background-color: #333; color: white; border: none; border-radius: 4px; padding: 8px 16px; font-weight: 600; }
    QPushButton:hover { background-color: #444; }
    QPushButton#PrimaryAction { background-color: #007acc; }
    QPushButton#PrimaryAction:hover { background-color: #0062a3; }
    QPushButton#Destructive { color: #ff6b6b; background-color: transparent; border: 1px solid #ff6b6b; }
    QPushButton#Destructive:hover { background-color: #3a1010; }
    QTabWidget::pane { border: 1px solid #3d3d3d; border-radius: 4px; }
    QTabBar::tab { background: #2d2d2d; color: #aaa; padding: 8px 12px; border-top-left-radius: 4px; border-top-right-radius: 4px; margin-right: 2px; }
    QTabBar::tab:selected { background: #1e1e1e; color: #007acc; border-bottom: 2px solid #007acc; }
    """

class InspectorPanel(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("Panel")
        self._current_meta: Optional[MediaMetadata] = None
        self._init_ui()

    def _init_ui(self):
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        # --- LEFT COLUMN: PREVIEW ---
        left_col = QWidget()
        left_layout = QVBoxLayout(left_col)
        
        self.thumb_lbl = QLabel("No Preview")
        self.thumb_lbl.setFixedSize(320, 180)
        self.thumb_lbl.setStyleSheet("background-color: #000; border: 1px solid #333; color: #666;")
        self.thumb_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.thumb_lbl.setScaledContents(True)
        
        self.stats_lbl = QLabel("Ready to Analyze")
        self.stats_lbl.setStyleSheet("color: #aaa; font-size: 12px; margin-top: 10px;")
        self.stats_lbl.setWordWrap(True)
        
        left_layout.addWidget(self.thumb_lbl)
        left_layout.addWidget(self.stats_lbl)
        left_layout.addStretch()

        # --- RIGHT COLUMN: TABS (Audio/Video Separation) ---
        right_container = QWidget()
        right_layout = QVBoxLayout(right_container)
        
        # 1. Media Type Selector
        type_grp = QGroupBox("Select Media Type")
        type_layout = QHBoxLayout()
        self.rb_video = QRadioButton("Video + Audio")
        self.rb_audio = QRadioButton("Audio Only")
        self.rb_audio.setChecked(True)
        
        self.btn_grp_type = QButtonGroup()
        self.btn_grp_type.addButton(self.rb_video)
        self.btn_grp_type.addButton(self.rb_audio)
        
        self.rb_video.toggled.connect(self._update_ui_mode)
        self.rb_audio.toggled.connect(self._update_ui_mode)
        
        type_layout.addWidget(self.rb_audio)
        type_layout.addWidget(self.rb_video)
        type_layout.addStretch()
        type_grp.setLayout(type_layout)
        
        # 2. Tab Widget for Categorization
        self.tabs = QTabWidget()
        
        # TAB 1: FORMAT & QUALITY
        tab_format = QWidget()
        fmt_layout = QVBoxLayout(tab_format)
        fmt_grid = QGridLayout()
        
        # Common
        self.cb_container = QComboBox()
        self.cb_container.currentTextChanged.connect(self._on_container_changed)
        
        # Audio Specific
        self.cb_abitrate = QComboBox()
        for k in ["320", "256", "192", "128", "96"]:
            self.cb_abitrate.addItem(f"{k} kbps", k)
            
        self.sb_asr = QSpinBox()
        self.sb_asr.setRange(0, 192000)
        self.sb_asr.setSpecialValueText("Auto")
        self.sb_asr.setSuffix(" Hz")
        
        # Video Specific
        self.cb_quality = QComboBox()
        self.cb_quality.addItems(["Best Available", "4K", "2K", "1080p", "720p", "480p"])
        
        self.cb_vcodec = QComboBox()
        self.cb_vcodec.addItems(["Best", "H264", "VP9", "AV1"])
        
        self.cb_acodec = QComboBox()
        self.cb_acodec.addItems(["Best", "AAC", "MP3", "Opus"])

        self.lbl_container = QLabel("Format:")
        self.lbl_abitrate = QLabel("Bitrate:")
        self.lbl_asr = QLabel("Sample Rate:")
        self.lbl_quality = QLabel("Resolution:")
        self.lbl_vcodec = QLabel("Video Codec:")
        self.lbl_acodec = QLabel("Audio Codec:")
        
        fmt_grid.addWidget(self.lbl_container, 0, 0)
        fmt_grid.addWidget(self.cb_container, 0, 1)
        
        fmt_grid.addWidget(self.lbl_quality, 1, 0)
        fmt_grid.addWidget(self.cb_quality, 1, 1)
        fmt_grid.addWidget(self.lbl_vcodec, 1, 2)
        fmt_grid.addWidget(self.cb_vcodec, 1, 3)
        fmt_grid.addWidget(self.lbl_acodec, 2, 0)
        fmt_grid.addWidget(self.cb_acodec, 2, 1)
        
        fmt_grid.addWidget(self.lbl_abitrate, 3, 0)
        fmt_grid.addWidget(self.cb_abitrate, 3, 1)
        fmt_grid.addWidget(self.lbl_asr, 3, 2)
        fmt_grid.addWidget(self.sb_asr, 3, 3)
        
        fmt_layout.addLayout(fmt_grid)
        fmt_layout.addStretch()
        
        # TAB 2: METADATA & FILE
        tab_meta = QWidget()
        meta_scroll = QScrollArea()
        meta_scroll.setWidgetResizable(True)
        meta_scroll.setFrameShape(QFrame.Shape.NoFrame)
        meta_content = QWidget()
        meta_form = QFormLayout(meta_content)
        
        self.in_filename = QLineEdit()
        self.in_filename.setPlaceholderText("Output filename (without extension)")
        
        self.in_title = QLineEdit()
        self.in_artist = QLineEdit()
        self.in_album = QLineEdit()
        self.in_genre = QLineEdit()
        self.in_date = QLineEdit()
        self.in_desc = QPlainTextEdit()
        self.in_desc.setFixedHeight(60)
        
        meta_form.addRow("Filename:", self.in_filename)
        meta_form.addRow("Title:", self.in_title)
        meta_form.addRow("Artist:", self.in_artist)
        meta_form.addRow("Album:", self.in_album)
        meta_form.addRow("Genre:", self.in_genre)
        meta_form.addRow("Date (YYYY):", self.in_date)
        meta_form.addRow("Description:", self.in_desc)
        
        meta_scroll.setWidget(meta_content)
        meta_layout = QVBoxLayout(tab_meta)
        meta_layout.addWidget(meta_scroll)

        # TAB 3: ADVANCED
        tab_adv = QWidget()
        adv_layout = QVBoxLayout(tab_adv)
        
        self.chk_meta = QCheckBox("Embed Metadata")
        self.chk_thumb = QCheckBox("Embed Thumbnail")
        self.chk_subs = QCheckBox("Download Subtitles")
        self.chk_norm = QCheckBox("Audio Normalization")
        self.chk_cookies = QCheckBox("Use Browser Cookies")
        
        self.chk_meta.setChecked(True)
        self.chk_thumb.setChecked(True)
        self.chk_cookies.setToolTip("Attempts to extract cookies from Chrome to bypass 403 Forbidden errors.")
        
        adv_layout.addWidget(self.chk_meta)
        adv_layout.addWidget(self.chk_thumb)
        adv_layout.addWidget(self.chk_subs)
        adv_layout.addWidget(self.chk_norm)
        adv_layout.addWidget(self.chk_cookies)
        adv_layout.addStretch()

        self.tabs.addTab(tab_format, "Format & Quality")
        self.tabs.addTab(tab_meta, "Metadata & File")
        self.tabs.addTab(tab_adv, "Advanced")

        right_layout.addWidget(type_grp)
        right_layout.addWidget(self.tabs)
        
        main_layout.addWidget(left_col, 1)
        main_layout.addWidget(right_container, 2)
        
        self._update_ui_mode()

    def set_metadata(self, meta: MediaMetadata):
        self._current_meta = meta
        safe_filename = re.sub(r'[\\/*?:"<>|]', "", meta.title)
        self.in_filename.setText(safe_filename)
        self.in_title.setText(meta.title)
        self.in_artist.setText(meta.artist)
        self.in_album.setText(meta.album)
        self.in_date.setText(meta.upload_date[:4] if meta.upload_date else "")
        self.in_desc.setPlainText(meta.description)
        self.in_genre.clear()
        
        self._recalc_estimate()

    def set_thumbnail(self, pixmap: QPixmap):
        self.thumb_lbl.setPixmap(pixmap)

    def _update_ui_mode(self):
        is_video = self.rb_video.isChecked()
        
        self.cb_container.blockSignals(True)
        self.cb_container.clear()
        if is_video:
            self.cb_container.addItems(["mp4", "mkv", "webm"])
        else:
            self.cb_container.addItems(["mp3", "flac", "wav", "m4a", "opus"])
        self.cb_container.blockSignals(False)
        self.cb_container.setCurrentIndex(0)
        
        video_widgets = [self.lbl_quality, self.cb_quality, self.lbl_vcodec, self.cb_vcodec, self.lbl_acodec, self.cb_acodec]
        for w in video_widgets:
            w.setVisible(is_video)
            
        audio_widgets = [self.lbl_abitrate, self.cb_abitrate, self.lbl_asr, self.sb_asr]
        for w in audio_widgets:
            w.setVisible(not is_video)
            
        self.chk_norm.setVisible(not is_video)
        
        self._on_container_changed(self.cb_container.currentText())

    def _on_container_changed(self, fmt: str):
        is_lossless = fmt in ['flac', 'wav']
        self.cb_abitrate.setEnabled(not is_lossless)
        if is_lossless:
            self.lbl_abitrate.setText("Bitrate (Lossless):")
        else:
            self.lbl_abitrate.setText("Bitrate:")
            
        self._recalc_estimate()

    def _recalc_estimate(self):
        if not self._current_meta: return
        base_info = (
            f"<b>Duration:</b> {self._current_meta.display_duration}<br>"
            f"<b>Source:</b> {self._current_meta.width}x{self._current_meta.height} @ {self._current_meta.fps}fps<br>"
            f"<b>Channel:</b> {self._current_meta.channel}"
        )
        self.stats_lbl.setText(base_info)

    def get_config_delta(self) -> Dict[str, Any]:
        return {
            'media_type': MediaType.VIDEO if self.rb_video.isChecked() else MediaType.AUDIO,
            'format_container': self.cb_container.currentText(),
            'video_codec': self.cb_vcodec.currentText().lower(),
            'audio_codec': self.cb_acodec.currentText().lower(),
            'quality_preset': self.cb_quality.currentText(),
            'audio_bitrate': self.cb_abitrate.currentData() if self.cb_abitrate.isEnabled() else "0",
            'audio_sample_rate': self.sb_asr.value(),
            'custom_filename': self.in_filename.text().strip() or "output",
            'meta_title': self.in_title.text(),
            'meta_artist': self.in_artist.text(),
            'meta_album': self.in_album.text(),
            'meta_genre': self.in_genre.text(),
            'meta_date': self.in_date.text(),
            'meta_desc': self.in_desc.toPlainText(),
            'embed_meta': self.chk_meta.isChecked(),
            'embed_thumb': self.chk_thumb.isChecked(),
            'embed_subs': self.chk_subs.isChecked(),
            'norm_audio': self.chk_norm.isChecked(),
            'use_cookies': self.chk_cookies.isChecked(),
        }

# --- MAIN CONTROLLER ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {VERSION}")
        self.resize(1200, 900)
        self.setMinimumSize(1000, 700)
        
        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(MAX_CONCURRENT_DOWNLOADS)
        self.active_runnables: Dict[str, DownloadRunnable] = {}
        self._current_meta: Optional[MediaMetadata] = None
        
        self.qt_log_handler = QtLogHandler()
        logging.getLogger().addHandler(self.qt_log_handler)
        
        self.init_ui()
        self.setStyleSheet(ModernStyle.STYLESHEET)

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        
        main_splitter = QSplitter(Qt.Orientation.Vertical)
        top_container = QWidget()
        main_layout = QVBoxLayout(top_container)
        main_layout.setContentsMargins(20, 20, 20, 0)
        top_bar = QHBoxLayout()
        header = QLabel(APP_NAME.upper())
        header.setStyleSheet("color: #007acc; font-size: 24px; font-weight: 800; letter-spacing: 2px;")
        
        self.chk_dev_mode = QCheckBox("Developer Mode")
        self.chk_dev_mode.toggled.connect(self.toggle_dev_mode)
        
        top_bar.addWidget(header)
        top_bar.addStretch()
        top_bar.addWidget(self.chk_dev_mode)
        main_layout.addLayout(top_bar)

        input_frame = QFrame()
        input_frame.setObjectName("Panel")
        input_layout = QHBoxLayout(input_frame)
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Paste URL (YouTube, Vimeo, SoundCloud)...")
        self.btn_analyze = QPushButton("Analyze Media")
        self.btn_analyze.setObjectName("PrimaryAction")
        self.btn_analyze.setFixedHeight(40)
        self.btn_analyze.clicked.connect(self.start_analysis)
        input_layout.addWidget(self.url_input, 1)
        input_layout.addWidget(self.btn_analyze)
        main_layout.addWidget(input_frame)

        self.inspector = InspectorPanel()
        self.inspector.setVisible(False)
        main_layout.addWidget(self.inspector)

        self.action_bar = QFrame()
        self.action_bar.setVisible(False)
        action_layout = QHBoxLayout(self.action_bar)
        self.path_input = QLineEdit(str(DEFAULT_DOWNLOAD_DIR))
        self.path_input.setReadOnly(True)
        btn_path = QPushButton("Change Folder")
        btn_path.clicked.connect(self.browse_folder)
        self.btn_queue = QPushButton("Add to Queue")
        self.btn_queue.setObjectName("PrimaryAction")
        self.btn_queue.clicked.connect(self.queue_download)
        action_layout.addWidget(QLabel("Output Folder:"))
        action_layout.addWidget(self.path_input)
        action_layout.addWidget(btn_path)
        action_layout.addStretch()
        action_layout.addWidget(self.btn_queue)
        main_layout.addWidget(self.action_bar)

        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Filename / Title", "Format", "Status", "Progress", "Actions"])
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        main_layout.addWidget(self.table)
        
        self.log_viewer = LogViewerWidget()
        self.log_viewer.setVisible(False)
        self.qt_log_handler.log_record.connect(self.log_viewer.append_log)
        
        main_splitter.addWidget(top_container)
        main_splitter.addWidget(self.log_viewer)
        main_splitter.setSizes([800, 200])
        main_splitter.setCollapsible(0, False)
        
        final_layout = QVBoxLayout(central)
        final_layout.setContentsMargins(0,0,0,0)
        final_layout.addWidget(main_splitter)

    def toggle_dev_mode(self, checked: bool):
        logging.getLogger().setLevel(logging.DEBUG if checked else logging.INFO)
        self.log_viewer.setVisible(checked)

    def start_analysis(self):
        url = self.url_input.text().strip()
        if not url: return
        
        # --- VALIDATOR ---
        if not YtDlpService.validate_url(url):
            QMessageBox.warning(self, "Invalid URL", "The provided URL pattern is not supported.\nPlease verify the link and try again.")
            return
        
        self.btn_analyze.setEnabled(False)
        self.btn_analyze.setText("Fetching...")
        worker = AnalysisRunnable(url)
        worker.signals.result.connect(self.on_analysis_success)
        worker.signals.thumbnail_data.connect(self.on_thumbnail_ready)
        worker.signals.error.connect(self.on_analysis_error)
        worker.signals.finished.connect(lambda: self.btn_analyze.setEnabled(True))
        worker.signals.finished.connect(lambda: self.btn_analyze.setText("Analyze Media"))
        self.thread_pool.start(worker)

    @pyqtSlot(object)
    def on_analysis_success(self, meta: MediaMetadata):
        self._current_meta = meta
        self.inspector.set_metadata(meta)
        self.inspector.setVisible(True)
        self.action_bar.setVisible(True)

    @pyqtSlot(bytes)
    def on_thumbnail_ready(self, data: bytes):
        pixmap = QPixmap()
        pixmap.loadFromData(data)
        self.inspector.set_thumbnail(pixmap)

    @pyqtSlot(str)
    def on_analysis_error(self, err_msg: str):
        QMessageBox.critical(self, "Analysis Failed", err_msg)
        self.inspector.setVisible(False)
        self.action_bar.setVisible(False)

    def browse_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Save Location", self.path_input.text())
        if d: self.path_input.setText(d)

    def queue_download(self):
        if not self._current_meta: return
        data = self.inspector.get_config_delta()
        
        config = DownloadJobConfig(
            job_id=str(uuid.uuid4()),
            url=self.url_input.text().strip(),
            output_path=Path(self.path_input.text()),
            media_type=data['media_type'],
            format_container=data['format_container'],
            audio_codec=data['audio_codec'],
            video_codec=data['video_codec'],
            quality_preset=data['quality_preset'],
            audio_sample_rate=data['audio_sample_rate'],
            audio_bitrate=str(data['audio_bitrate']),
            
            custom_filename=data['custom_filename'],
            meta_title=data['meta_title'],
            meta_artist=data['meta_artist'],
            meta_album=data['meta_album'],
            meta_genre=data['meta_genre'],
            meta_date=data['meta_date'],
            meta_desc=data['meta_desc'],
            
            embed_metadata=data['embed_meta'],
            embed_thumbnail=data['embed_thumb'],
            embed_subs=data['embed_subs'],
            normalize_audio=data['norm_audio'],
            use_browser_cookies=data['use_cookies']
        )
        self._spawn_download(config)
        
        self.inspector.setVisible(False)
        self.action_bar.setVisible(False)
        self.url_input.clear()

    def _spawn_download(self, config: DownloadJobConfig):
        runnable = DownloadRunnable(config)
        runnable.signals.progress.connect(self.update_progress)
        runnable.signals.status.connect(self.update_status)
        runnable.signals.finished.connect(lambda: self.on_job_finished(config.job_id))
        runnable.signals.error.connect(lambda err: self.on_job_error(config.job_id, err))
        self.active_runnables[config.job_id] = runnable
        self.add_table_row(config)
        self.thread_pool.start(runnable)

    def add_table_row(self, config: DownloadJobConfig):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        display_name = f"{config.custom_filename}.{config.format_container}"
        title_item = QTableWidgetItem(display_name)
        title_item.setToolTip(config.meta_title)
        title_item.setData(Qt.ItemDataRole.UserRole, config.job_id)
        
        fmt_str = config.format_container.upper()
        if config.media_type == MediaType.VIDEO:
            fmt_str += f" ({config.quality_preset})"
            
        pbar = QProgressBar()
        pbar.setValue(0)
        btn_cancel = QPushButton("Stop")
        btn_cancel.setObjectName("Destructive")
        btn_cancel.clicked.connect(lambda: self.cancel_job(config.job_id))
        
        self.table.setItem(row, 0, title_item)
        self.table.setItem(row, 1, QTableWidgetItem(fmt_str))
        self.table.setItem(row, 2, QTableWidgetItem("Queued"))
        self.table.setCellWidget(row, 3, pbar)
        self.table.setCellWidget(row, 4, btn_cancel)

    def get_row_by_id(self, job_id: str) -> int:
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item and item.data(Qt.ItemDataRole.UserRole) == job_id: return r
        return -1

    @pyqtSlot(str, float, str)
    def update_progress(self, job_id: str, pct: float, speed: str):
        row = self.get_row_by_id(job_id)
        if row >= 0:
            self.table.cellWidget(row, 3).setValue(int(pct))
            self.table.item(row, 2).setText(f"▼ {speed}")

    @pyqtSlot(str, str)
    def update_status(self, job_id: str, msg: str):
        row = self.get_row_by_id(job_id)
        if row >= 0: self.table.item(row, 2).setText(msg)

    def on_job_finished(self, job_id: str):
        self._cleanup_job(job_id, "✔ Done", QColor("#4caf50"))

    def on_job_error(self, job_id: str, err: str):
        self._cleanup_job(job_id, "✘ Error", QColor("#f44336"))

    def cancel_job(self, job_id: str):
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            self.update_status(job_id, "Stopping...")

    def _cleanup_job(self, job_id: str, status_text: str, color: QColor):
        row = self.get_row_by_id(job_id)
        if row >= 0:
            item = self.table.item(row, 2)
            item.setText(status_text)
            item.setForeground(color)
            if "Done" in status_text: self.table.cellWidget(row, 3).setValue(100)
            self.table.setCellWidget(row, 4, None)
        if job_id in self.active_runnables: del self.active_runnables[job_id]

def main():
    sys.excepthook = lambda t, v, tb: logging.critical("Uncaught:", exc_info=(t, v, tb))
    logging.basicConfig(level=logging.INFO)
    app = QApplication(sys.argv)
    app.setStyle(QStyleFactory.create("Fusion"))
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()