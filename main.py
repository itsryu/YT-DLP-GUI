import sys
import logging
import traceback
import uuid
import re
import shutil
import requests
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List
from enum import Enum

try:
    import yt_dlp
    from yt_dlp.utils import DownloadError
except ImportError:
    print("CRITICAL: 'yt_dlp' library missing. pip install yt-dlp")
    sys.exit(1)

try:
    import mutagen
except ImportError:
    print("CRITICAL: 'mutagen' library missing. pip install mutagen")
    sys.exit(1)

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QAbstractItemView, QMessageBox, QFrame, QScrollArea,
    QGroupBox, QFormLayout, QCheckBox, QSpinBox, QDoubleSpinBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QObject
from PyQt6.QtGui import QColor, QPixmap

# --- CONSTANTS & CONFIG ---
APP_NAME = "SoundStream Pro"
VERSION = "5.2.1 (Initialization Fix)"
DEFAULT_DOWNLOAD_DIR = str(Path.home() / "Downloads")
LOG_FILENAME = "app_debug.log"

# --- LOGGING SETUP ---
def setup_logging() -> None:
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler(
        LOG_FILENAME, maxBytes=5*1024*1024, backupCount=3, encoding='utf-8'
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    logging.info("=== Application Started ===")

def global_exception_hook(exc_type, exc_value, exc_traceback) -> None:
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logging.critical("Uncaught Exception:", exc_info=(exc_type, exc_value, exc_traceback))
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

# --- DATA STRUCTURES ---
class MediaType(Enum):
    AUDIO = "Audio"
    VIDEO = "Video"

@dataclass
class DownloadJobConfig:
    id: str
    url: str
    output_path: str
    media_type: MediaType
    # Technical Options
    format_container: str  # Target container/extension (mp3, mp4, etc)
    audio_codec: str       # Internal stream codec
    video_codec: str       # Internal stream codec
    quality_preset: str    # Resolution for video
    audio_sample_rate: int # Hz
    audio_bitrate: str     # 320k, 128k, or '0' for best/lossless
    # Metadata Overrides
    meta_title: str
    meta_artist: str
    meta_album: str
    # Flags
    embed_metadata: bool
    embed_thumbnail: bool
    embed_subs: bool
    normalize_audio: bool

# --- WORKERS ---

class AnalysisSignals(QObject):
    finished = pyqtSignal(object)  # Returns dict (info)
    error = pyqtSignal(str)
    thumbnail_ready = pyqtSignal(QPixmap)

class AnalysisWorker(QThread):
    def __init__(self, url: str):
        super().__init__()
        self.url = url
        self.signals = AnalysisSignals()

    def run(self):
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(self.url, download=False)
                if 'entries' in info: info = info['entries'][0]
                
                self.signals.finished.emit(info)

                thumb_url = info.get('thumbnail')
                if thumb_url:
                    try:
                        resp = requests.get(thumb_url, stream=True, timeout=5)
                        if resp.status_code == 200:
                            pixmap = QPixmap()
                            pixmap.loadFromData(resp.content)
                            self.signals.thumbnail_ready.emit(pixmap)
                    except Exception as e:
                        logging.warning(f"Thumbnail fetch error: {e}")
        except Exception as e:
            self.signals.error.emit(str(e))

class DownloadSignals(QObject):
    started = pyqtSignal(str)
    progress = pyqtSignal(str, float, str)
    finished = pyqtSignal(str)
    error = pyqtSignal(str, str)
    status_update = pyqtSignal(str, str)
    cancelled = pyqtSignal(str)

class DownloadWorker(QThread):
    def __init__(self, config: DownloadJobConfig):
        super().__init__()
        self.config = config
        self.signals = DownloadSignals()
        self._is_cancelled = False
        self.info: Optional[Dict[str, Any]] = None

    def run(self) -> None:
        temp_dir = Path(self.config.output_path) / ".inprogress" / self.config.id
        
        try:
            ydl_opts = self._build_options(temp_dir)
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                self.signals.status_update.emit(self.config.id, "Initializing...")
                
                info = ydl.extract_info(self.config.url, download=False)
                if 'entries' in info: info = info['entries'][0]
                self.info = info

                self.signals.started.emit(self.config.id)

                if self._is_cancelled: raise DownloadError("Cancelled.")

                logging.info(f"[{self.config.id}] Downloading to sandbox: {temp_dir}")
                ydl.download([info['webpage_url']])
            
            if self._is_cancelled:
                self._cleanup_sandbox(temp_dir)
                self.signals.cancelled.emit(self.config.id)
                return

            self._finalize(temp_dir)
            self.signals.finished.emit(self.config.id)

        except Exception as e:
            if self._is_cancelled:
                self._cleanup_sandbox(temp_dir)
                self.signals.cancelled.emit(self.config.id)
            else:
                logging.error(f"Job {self.config.id} failed: {e}", exc_info=True)
                self._cleanup_sandbox(temp_dir)
                self.signals.error.emit(self.config.id, str(e))

    def cancel(self):
        self._is_cancelled = True

    def _cleanup_sandbox(self, temp_dir: Path):
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except OSError as e:
                logging.error(f"Cleanup failed: {e}")

    def _finalize(self, temp_dir: Path):
        dest = Path(self.config.output_path)
        dest.mkdir(parents=True, exist_ok=True)
        
        if not temp_dir.exists(): return

        for file_p in temp_dir.iterdir():
            if file_p.is_file():
                target = dest / file_p.name
                if target.exists():
                    try: target.unlink() 
                    except OSError: pass
                shutil.move(str(file_p), str(target))
        
        shutil.rmtree(temp_dir, ignore_errors=True)

    def _progress_hook(self, d):
        if self._is_cancelled: raise DownloadError("Cancelled")
        if d['status'] == 'downloading':
            p = d.get('_percent_str', '0%').replace('%','')
            try: percent = float(p)
            except: percent = 0.0
            self.signals.progress.emit(self.config.id, percent, d.get('_speed_str', 'N/A'))
            self.signals.status_update.emit(self.config.id, "Downloading...")
        elif d['status'] == 'finished':
            self.signals.progress.emit(self.config.id, 100.0, "Processing")
            self.signals.status_update.emit(self.config.id, "Encoding/Tagging...")

    def _build_options(self, target_folder: Path) -> Dict[str, Any]:
        target_folder.mkdir(parents=True, exist_ok=True)
        out_tmpl = str(target_folder / "%(title)s.%(ext)s")

        opts = {
            'outtmpl': out_tmpl,
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
            'ignoreerrors': True,
            'sleep_interval': 2,
            'max_sleep_interval': 5,
            'extractor_args': {'youtube': {'player_client': ['android', 'ios', 'web']}},
            
            'writethumbnail': self.config.embed_thumbnail,
            'addmetadata': self.config.embed_metadata,
            
            'writesubtitles': self.config.embed_subs,
            'writeautomaticsub': self.config.embed_subs,
            'subtitleslangs': ['en', 'pt', 'all'] if self.config.embed_subs else [],

            'postprocessors': [],
            'postprocessor_args': {},
        }

        # --- FORMAT LOGIC ---
        if self.config.media_type == MediaType.AUDIO:
            opts['format'] = 'bestaudio/best'
            
            # Post-processor for Audio Conversion
            audio_args = []
            
            # Sample Rate
            if self.config.audio_sample_rate > 0:
                audio_args.extend(['-ar', str(self.config.audio_sample_rate)])
                
            opts['postprocessors'].append({
                'key': 'FFmpegExtractAudio',
                'preferredcodec': self.config.format_container, 
                # If explicit bitrate is provided (e.g. '320'), pass it. 
                # '0' means 'best' in ffmpeg postprocessor context for quality, 
                # but for mp3/aac we pass the kbit/s value if available.
                'preferredquality': self.config.audio_bitrate if self.config.audio_bitrate != '0' else None,
            })
            
            if audio_args:
                if 'FFmpegExtractAudio' not in opts['postprocessor_args']:
                    opts['postprocessor_args']['FFmpegExtractAudio'] = []
                opts['postprocessor_args']['FFmpegExtractAudio'].extend(audio_args)

        elif self.config.media_type == MediaType.VIDEO:
            ext = self.config.format_container 
            
            res_map = {"4K": 2160, "2K": 1440, "1080p": 1080, "720p": 720, "480p": 480}
            target_h = res_map.get(self.config.quality_preset)
            res_str = f"[height<={target_h}]" if target_h else ""
            
            v_codec_sel = self.config.video_codec
            a_codec_sel = self.config.audio_codec
            
            v_fmt = f"bestvideo{res_str}"
            if v_codec_sel == 'h264': v_fmt += "[vcodec^=avc]"
            elif v_codec_sel == 'vp9': v_fmt += "[vcodec^=vp9]"
            elif v_codec_sel == 'av1': v_fmt += "[vcodec^=av01]"
            
            a_fmt = "bestaudio"
            if a_codec_sel != 'best':
                a_fmt = f"bestaudio[acodec^={a_codec_sel}]/bestaudio"

            opts['format'] = f"{v_fmt}+{a_fmt}/best{res_str}"
            opts['merge_output_format'] = ext

        # --- METADATA ---
        meta_args = []
        if self.config.meta_title: meta_args.extend(['-metadata', f'title={self.config.meta_title}'])
        if self.config.meta_artist: meta_args.extend(['-metadata', f'artist={self.config.meta_artist}'])
        if self.config.meta_album: meta_args.extend(['-metadata', f'album={self.config.meta_album}'])
        
        if self.config.embed_metadata:
            opts['postprocessors'].append({
                'key': 'FFmpegMetadata',
                'add_chapters': True,
                'add_metadata': True,
            })
            if meta_args:
                if 'FFmpegMetadata' not in opts['postprocessor_args']:
                    opts['postprocessor_args']['FFmpegMetadata'] = []
                opts['postprocessor_args']['FFmpegMetadata'].extend(meta_args)

        # --- THUMBNAIL ---
        if self.config.embed_thumbnail:
            opts['postprocessors'].append({
                'key': 'FFmpegThumbnailsConvertor',
                'format': 'jpg',
            })
            if 'FFmpegThumbnailsConvertor' not in opts['postprocessor_args']:
                opts['postprocessor_args']['FFmpegThumbnailsConvertor'] = []
            opts['postprocessor_args']['FFmpegThumbnailsConvertor'].extend(['-q:v', '1'])
            
            opts['postprocessors'].append({'key': 'EmbedThumbnail'})

        # --- NORMALIZATION ---
        if self.config.normalize_audio and self.config.media_type == MediaType.AUDIO:
            if 'FFmpegExtractAudio' not in opts['postprocessor_args']:
                opts['postprocessor_args']['FFmpegExtractAudio'] = []
            opts['postprocessor_args']['FFmpegExtractAudio'].extend(['-af', 'loudnorm=I=-14:TP=-1.5:LRA=11'])

        return opts

# --- UI COMPONENTS ---

class ModernStyle:
    STYLESHEET = """
    QMainWindow { background-color: #121212; }
    QWidget { color: #e0e0e0; font-family: 'Segoe UI', 'Roboto', sans-serif; font-size: 13px; }
    QFrame#Panel { background-color: #1e1e1e; border-radius: 8px; border: 1px solid #333; }
    QGroupBox { border: 1px solid #3d3d3d; border-radius: 6px; margin-top: 20px; font-weight: bold; color: #007acc; }
    QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; }
    QLineEdit, QComboBox, QSpinBox { background-color: #252526; border: 1px solid #3d3d3d; border-radius: 4px; padding: 6px; color: white; selection-background-color: #007acc; }
    QLineEdit:focus, QComboBox:focus, QSpinBox:focus { border: 1px solid #007acc; }
    QPushButton { background-color: #333; color: white; border: none; border-radius: 4px; padding: 8px 16px; font-weight: 600; }
    QPushButton:hover { background-color: #444; }
    QPushButton:pressed { background-color: #222; }
    QPushButton#PrimaryAction { background-color: #007acc; font-size: 14px; }
    QPushButton#PrimaryAction:hover { background-color: #0062a3; }
    QPushButton#Destructive { color: #ff6b6b; background-color: transparent; border: 1px solid #ff6b6b; }
    QPushButton#Destructive:hover { background-color: #3a1010; }
    QTableWidget { background-color: #1e1e1e; gridline-color: #333; border: none; outline: none; }
    QHeaderView::section { background-color: #252526; padding: 6px; border: none; border-bottom: 2px solid #007acc; color: #ccc; }
    """

class InspectorPanel(QFrame):
    def __init__(self):
        super().__init__()
        self.setObjectName("Panel")
        self.layout = QHBoxLayout(self)
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.layout.setSpacing(20)
        self.current_duration = 0 # seconds
        self.base_info_str = "Ready to Analyze"  # Initialize with default value
        
        # Left: Visuals
        left_col = QWidget()
        left_layout = QVBoxLayout(left_col)
        self.thumb_lbl = QLabel("No Preview")
        self.thumb_lbl.setFixedSize(320, 180)
        self.thumb_lbl.setStyleSheet("background-color: #000; border: 1px solid #333;")
        self.thumb_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.thumb_lbl.setScaledContents(True)
        self.stats_lbl = QLabel(self.base_info_str)
        self.stats_lbl.setStyleSheet("color: #aaa; font-size: 12px; margin-top: 10px;")
        self.stats_lbl.setWordWrap(True)
        left_layout.addWidget(self.thumb_lbl)
        left_layout.addWidget(self.stats_lbl)
        left_layout.addStretch()
        
        # Right: Editors
        right_col = QScrollArea()
        right_col.setWidgetResizable(True)
        right_col.setFrameShape(QFrame.Shape.NoFrame)
        right_content = QWidget()
        right_layout = QVBoxLayout(right_content)
        
        # 1. Metadata
        meta_grp = QGroupBox("Metadata Editor")
        meta_form = QFormLayout()
        self.in_title = QLineEdit()
        self.in_artist = QLineEdit()
        self.in_album = QLineEdit()
        meta_form.addRow("Title:", self.in_title)
        meta_form.addRow("Artist:", self.in_artist)
        meta_form.addRow("Album:", self.in_album)
        meta_grp.setLayout(meta_form)
        
        # 2. Technical Options
        tech_grp = QGroupBox("Format & Quality")
        tech_grid = QGridLayout()
        
        # --- Audio Inputs ---
        self.cb_container = QComboBox()
        self.cb_container.addItems(["mp3", "flac", "wav", "opus", "m4a", "mp4", "mkv", "webm"])
        self.cb_container.currentTextChanged.connect(self.on_container_changed)
        
        # Audio Bitrate
        self.lbl_abitrate = QLabel("Audio Bitrate:")
        self.cb_abitrate = QComboBox()
        # Default keys for kbps
        self.cb_abitrate.addItem("320k (High)", "320")
        self.cb_abitrate.addItem("256k", "256")
        self.cb_abitrate.addItem("192k", "192")
        self.cb_abitrate.addItem("128k (Std)", "128")
        self.cb_abitrate.addItem("96k", "96")
        self.cb_abitrate.addItem("64k", "64")
        self.cb_abitrate.currentIndexChanged.connect(self.recalc_size_estimate)
        
        self.lbl_asr = QLabel("Sample Rate:")
        self.sb_asr = QSpinBox()
        self.sb_asr.setRange(0, 192000)
        self.sb_asr.setValue(0)
        self.sb_asr.setSpecialValueText("Auto")
        self.sb_asr.setSuffix(" Hz")
        
        # --- Video Inputs ---
        self.lbl_quality = QLabel("Quality:")
        self.cb_quality = QComboBox()
        self.cb_quality.addItems(["Best Available", "4K", "2K", "1080p", "720p", "480p"])
        self.cb_quality.currentIndexChanged.connect(self.recalc_size_estimate)
        
        self.lbl_vcodec = QLabel("Video Codec:")
        self.cb_vcodec = QComboBox()
        self.cb_vcodec.addItems(["best", "h264", "vp9", "av1"])
        
        self.lbl_acodec_st = QLabel("Stream Codec:")
        self.cb_acodec = QComboBox()
        self.cb_acodec.addItems(["best", "aac", "mp3", "opus"])
        
        # Layout Config
        tech_grid.addWidget(QLabel("Output Format:"), 0, 0)
        tech_grid.addWidget(self.cb_container, 0, 1)
        
        # Audio Rows
        tech_grid.addWidget(self.lbl_abitrate, 1, 0)
        tech_grid.addWidget(self.cb_abitrate, 1, 1)
        tech_grid.addWidget(self.lbl_asr, 1, 2)
        tech_grid.addWidget(self.sb_asr, 1, 3)
        
        # Video Rows
        tech_grid.addWidget(self.lbl_quality, 2, 0)
        tech_grid.addWidget(self.cb_quality, 2, 1)
        tech_grid.addWidget(self.lbl_vcodec, 2, 2)
        tech_grid.addWidget(self.cb_vcodec, 2, 3)
        
        tech_grid.addWidget(self.lbl_acodec_st, 3, 0)
        tech_grid.addWidget(self.cb_acodec, 3, 1)
        
        tech_grp.setLayout(tech_grid)
        
        # 3. Features
        feat_grp = QGroupBox("Processing")
        feat_layout = QHBoxLayout()
        self.chk_meta = QCheckBox("Embed Metadata")
        self.chk_thumb = QCheckBox("Embed Art")
        self.chk_subs = QCheckBox("Download Lyrics")
        self.chk_norm = QCheckBox("Loudness Norm.")
        self.chk_meta.setChecked(True)
        self.chk_thumb.setChecked(True)
        feat_layout.addWidget(self.chk_meta)
        feat_layout.addWidget(self.chk_thumb)
        feat_layout.addWidget(self.chk_subs)
        feat_layout.addWidget(self.chk_norm)
        feat_grp.setLayout(feat_layout)
        
        right_layout.addWidget(meta_grp)
        right_layout.addWidget(tech_grp)
        right_layout.addWidget(feat_grp)
        right_layout.addStretch()
        right_col.setWidget(right_content)
        
        self.layout.addWidget(left_col, 1)
        self.layout.addWidget(right_col, 2)
        
        # Trigger initial state
        self.on_container_changed("mp3")

    def on_container_changed(self, text: str):
        """Dynamically hides/shows options based on container type."""
        is_video = text in ['mp4', 'mkv', 'webm']
        is_lossless_audio = text in ['flac', 'wav']
        is_lossy_audio = text in ['mp3', 'm4a', 'opus']
        
        # Video Controls
        self.lbl_vcodec.setVisible(is_video)
        self.cb_vcodec.setVisible(is_video)
        self.lbl_quality.setVisible(is_video)
        self.cb_quality.setVisible(is_video)
        self.lbl_acodec_st.setVisible(is_video)
        self.cb_acodec.setVisible(is_video)
        
        # Audio Bitrate Control (Only for lossy audio)
        self.lbl_abitrate.setVisible(is_lossy_audio)
        self.cb_abitrate.setVisible(is_lossy_audio)
        
        # Sample Rate (Common for audio)
        self.lbl_asr.setVisible(not is_video)
        self.sb_asr.setVisible(not is_video)
        
        # Normalization (Audio only)
        self.chk_norm.setVisible(not is_video)
        
        self.recalc_size_estimate()

    def populate(self, info: Dict[str, Any]):
        self.in_title.setText(info.get('title', ''))
        self.in_artist.setText(info.get('artist', info.get('uploader', '')))
        self.in_album.setText(info.get('album', ''))
        
        self.current_duration = info.get('duration', 0)
        self.base_info_str = (
            f"<b>Duration:</b> {info.get('duration_string', 'N/A')}<br>"
            f"<b>Source Res:</b> {info.get('width','?')}x{info.get('height','?')}<br>"
            f"<b>Source FPS:</b> {info.get('fps', 'N/A')}<br>"
        )
        self.recalc_size_estimate()

    def recalc_size_estimate(self):
        """Calculates estimated file size based on selected bitrate and duration."""
        if self.current_duration == 0:
            self.stats_lbl.setText(self.base_info_str)
            return

        is_video = self.cb_quality.isVisible()
        is_lossless = self.cb_container.currentText() in ['flac', 'wav']
        
        total_bitrate_kbps = 0
        
        if is_video:
            # Heuristic for video bitrate based on resolution (very approx)
            # 4K ~ 45Mbps, 1080p ~ 6Mbps, 720p ~ 2.5Mbps
            q_text = self.cb_quality.currentText()
            if "4K" in q_text: total_bitrate_kbps += 45000
            elif "2K" in q_text: total_bitrate_kbps += 16000
            elif "1080p" in q_text: total_bitrate_kbps += 6000
            elif "720p" in q_text: total_bitrate_kbps += 2500
            elif "480p" in q_text: total_bitrate_kbps += 1000
            else: total_bitrate_kbps += 6000 # Default 'Best' guess
            
            # Add audio track estimate (approx 128k)
            total_bitrate_kbps += 128
            
        elif is_lossless:
            # FLAC/WAV ~ 1411kbps (CD quality base)
            total_bitrate_kbps = 1411
        else:
            # Lossy Audio - User defined
            try:
                total_bitrate_kbps = int(self.cb_abitrate.currentData())
            except:
                total_bitrate_kbps = 192 # Default fallback

        # Size (MB) = (Bitrate (kbps) * Duration (s)) / 8 / 1024
        est_size_mb = (total_bitrate_kbps * self.current_duration) / 8192
        
        size_str = f"<b>Est. Size:</b> ~{est_size_mb:.2f} MB"
        self.stats_lbl.setText(self.base_info_str + size_str)

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {VERSION}")
        self.resize(1200, 800)
        self.setMinimumSize(1000, 600)
        self.active_downloads: Dict[str, DownloadWorker] = {}
        self.init_ui()
        self.setStyleSheet(ModernStyle.STYLESHEET)
        setup_logging()

    def init_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # Header
        top_bar = QHBoxLayout()
        header = QLabel(APP_NAME.upper())
        header.setStyleSheet("color: #007acc; font-size: 24px; font-weight: 800; letter-spacing: 2px;")
        top_bar.addWidget(header)
        top_bar.addStretch()
        main_layout.addLayout(top_bar)

        # Input
        input_frame = QFrame()
        input_frame.setObjectName("Panel")
        input_layout = QHBoxLayout(input_frame)
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Paste URL to analyze...")
        self.url_input.setFixedHeight(40)
        self.btn_analyze = QPushButton("Analyze Media")
        self.btn_analyze.setObjectName("PrimaryAction")
        self.btn_analyze.setFixedHeight(40)
        self.btn_analyze.clicked.connect(self.start_analysis)
        input_layout.addWidget(self.url_input, 1)
        input_layout.addWidget(self.btn_analyze)
        main_layout.addWidget(input_frame)

        # Inspector
        self.inspector = InspectorPanel()
        self.inspector.setVisible(False)
        
        # Actions
        self.action_bar = QFrame()
        self.action_bar.setVisible(False)
        action_layout = QHBoxLayout(self.action_bar)
        action_layout.setContentsMargins(0,0,0,0)
        self.path_input = QLineEdit(DEFAULT_DOWNLOAD_DIR)
        self.path_input.setReadOnly(True)
        btn_path = QPushButton("Change Folder")
        btn_path.clicked.connect(self.browse_folder)
        self.btn_queue = QPushButton("Add to Queue & Download")
        self.btn_queue.setObjectName("PrimaryAction")
        self.btn_queue.clicked.connect(self.add_to_queue)
        action_layout.addWidget(QLabel("Save to:"))
        action_layout.addWidget(self.path_input)
        action_layout.addWidget(btn_path)
        action_layout.addStretch()
        action_layout.addWidget(self.btn_queue)

        main_layout.addWidget(self.inspector)
        main_layout.addWidget(self.action_bar)

        # Queue
        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Media Title", "Format", "Status", "Progress", "Actions"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for i in range(1, 5): self.table.horizontalHeader().setSectionResizeMode(i, QHeaderView.ResizeMode.Fixed)
        self.table.setColumnWidth(1, 100)
        self.table.setColumnWidth(2, 150)
        self.table.setColumnWidth(3, 200)
        self.table.setColumnWidth(4, 100)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        main_layout.addWidget(QLabel("Download Queue"))
        main_layout.addWidget(self.table)

    def start_analysis(self):
        url = self.url_input.text().strip()
        if not url: return
        self.btn_analyze.setText("Fetching...")
        self.btn_analyze.setEnabled(False)
        self.inspector.thumb_lbl.clear()
        self.inspector.thumb_lbl.setText("Loading...")
        
        self.analysis_worker = AnalysisWorker(url)
        self.analysis_worker.signals.finished.connect(self.on_analysis_done)
        self.analysis_worker.signals.thumbnail_ready.connect(lambda p: self.inspector.thumb_lbl.setPixmap(p))
        self.analysis_worker.signals.error.connect(self.on_analysis_error)
        self.analysis_worker.start()

    def on_analysis_done(self, info: Dict):
        self.inspector.setVisible(True)
        self.action_bar.setVisible(True)
        self.inspector.populate(info)
        self.btn_analyze.setText("Analyze Media")
        self.btn_analyze.setEnabled(True)

    def on_analysis_error(self, err: str):
        self.btn_analyze.setText("Analyze Media")
        self.btn_analyze.setEnabled(True)
        QMessageBox.critical(self, "Error", err)

    def browse_folder(self):
        d = QFileDialog.getExistingDirectory(self, "Save Location", self.path_input.text())
        if d: self.path_input.setText(d)

    def add_to_queue(self):
        url = self.url_input.text().strip()
        if not url: return

        container = self.inspector.cb_container.currentText()
        is_audio = container in ['mp3', 'flac', 'wav', 'opus', 'm4a']
        bitrate = "0"
        
        if self.inspector.cb_abitrate.isVisible():
            bitrate = self.inspector.cb_abitrate.currentData()

        config = DownloadJobConfig(
            id=str(uuid.uuid4()),
            url=url,
            output_path=self.path_input.text(),
            media_type=MediaType.AUDIO if is_audio else MediaType.VIDEO,
            format_container=container,
            audio_codec=self.inspector.cb_acodec.currentText(),
            video_codec=self.inspector.cb_vcodec.currentText(),
            quality_preset=self.inspector.cb_quality.currentText(),
            audio_sample_rate=self.inspector.sb_asr.value(),
            audio_bitrate=bitrate,
            meta_title=self.inspector.in_title.text(),
            meta_artist=self.inspector.in_artist.text(),
            meta_album=self.inspector.in_album.text(),
            embed_metadata=self.inspector.chk_meta.isChecked(),
            embed_thumbnail=self.inspector.chk_thumb.isChecked(),
            embed_subs=self.inspector.chk_subs.isChecked(),
            normalize_audio=self.inspector.chk_norm.isChecked()
        )

        self.start_download(config)
        self.inspector.setVisible(False)
        self.action_bar.setVisible(False)
        self.url_input.clear()

    def start_download(self, config: DownloadJobConfig):
        worker = DownloadWorker(config)
        worker.signals.started.connect(lambda j: self.update_row(j, status="Starting..."))
        worker.signals.progress.connect(self.update_progress)
        worker.signals.finished.connect(self.on_finished)
        worker.signals.error.connect(self.on_error)
        worker.signals.cancelled.connect(self.on_cancelled)
        worker.signals.status_update.connect(lambda j, m: self.update_row(j, status=m))
        
        self.active_downloads[config.id] = worker
        self.add_row(config)
        worker.start()

    def add_row(self, config: DownloadJobConfig):
        row = self.table.rowCount()
        self.table.insertRow(row)
        title = config.meta_title if config.meta_title else config.url
        
        item_title = QTableWidgetItem(title)
        item_title.setData(Qt.ItemDataRole.UserRole, config.id)
        
        fmt_disp = config.format_container.upper()
        if config.media_type == MediaType.VIDEO:
            fmt_disp += f" / {config.quality_preset}"
        elif config.audio_bitrate != "0":
            fmt_disp += f" / {config.audio_bitrate}k"
            
        item_fmt = QTableWidgetItem(fmt_disp)
        item_stat = QTableWidgetItem("Pending")
        
        pbar = QProgressBar()
        pbar.setValue(0)
        pbar.setTextVisible(False)
        
        btn = QPushButton("Stop")
        btn.setObjectName("Destructive")
        btn.clicked.connect(lambda: self.cancel_job(config.id))
        
        self.table.setItem(row, 0, item_title)
        self.table.setItem(row, 1, item_fmt)
        self.table.setItem(row, 2, item_stat)
        self.table.setCellWidget(row, 3, pbar)
        self.table.setCellWidget(row, 4, btn)

    def get_row(self, jid: str) -> int:
        for r in range(self.table.rowCount()):
            if self.table.item(r, 0).data(Qt.ItemDataRole.UserRole) == jid: return r
        return -1

    def update_row(self, jid: str, status: str = None):
        row = self.get_row(jid)
        if row >= 0 and status: self.table.item(row, 2).setText(status)

    def update_progress(self, jid: str, pct: float, speed: str):
        row = self.get_row(jid)
        if row >= 0:
            self.table.cellWidget(row, 3).setValue(int(pct)) # type: ignore
            self.table.item(row, 2).setText(f"▼ {speed}")

    def on_finished(self, jid: str):
        row = self.get_row(jid)
        if row >= 0:
            self.table.item(row, 2).setText("✔ Done")
            self.table.item(row, 2).setForeground(QColor("#4caf50"))
            self.table.cellWidget(row, 3).setValue(100) # type: ignore
            self.table.setCellWidget(row, 4, None)
        self.cleanup(jid)

    def on_error(self, jid: str, err: str):
        row = self.get_row(jid)
        if row >= 0:
            self.table.item(row, 2).setText("✘ Error")
            self.table.item(row, 2).setForeground(QColor("#f44336"))
            self.table.setCellWidget(row, 4, None)
        self.cleanup(jid)

    def on_cancelled(self, jid: str):
        row = self.get_row(jid)
        if row >= 0:
            self.table.item(row, 2).setText("🚫 Cancelled")
            self.table.item(row, 2).setForeground(QColor("#888"))
            self.table.cellWidget(row, 3).setValue(0) # type: ignore
            self.table.setCellWidget(row, 4, None)
        self.cleanup(jid)

    def cancel_job(self, jid: str):
        if jid in self.active_downloads:
            self.active_downloads[jid].cancel()
            self.update_row(jid, "Stopping...")

    def cleanup(self, jid: str):
        if jid in self.active_downloads:
            self.active_downloads[jid].quit()
            self.active_downloads[jid].wait()
            del self.active_downloads[jid]

if __name__ == "__main__":
    setup_logging()
    sys.excepthook = global_exception_hook

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    if hasattr(Qt.ApplicationAttribute, 'AA_EnableHighDpiScaling'):
        app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    if hasattr(Qt.ApplicationAttribute, 'AA_UseHighDpiPixmaps'):
        app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)

    win = MainWindow()
    win.show()
    
    sys.exit(app.exec())