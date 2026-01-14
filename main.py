import sys
import os
import logging
import traceback
import uuid
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from datetime import datetime

try:
    import yt_dlp
except ImportError:
    print("CRITICAL: 'yt_dlp' library missing. Please install: pip install yt-dlp")
    sys.exit(1)

try:
    import mutagen
except ImportError:
    print("CRITICAL: 'mutagen' library missing. Required for metadata/cover art features.")
    print("Please install: pip install mutagen")
    sys.exit(1)

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QAbstractItemView, QMessageBox, QFrame, QSizePolicy
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QObject
)
from PyQt6.QtGui import QIcon, QColor


APP_NAME = "SoundStream Pro"
VERSION = "3.8.0 (Metadata-Engine)"
DEFAULT_DOWNLOAD_DIR = str(Path.home() / "Downloads")
LOG_FILENAME = "app_debug.log"

def setup_logging():
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

def global_exception_hook(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logging.critical("Uncaught Exception detected:", exc_info=(exc_type, exc_value, exc_traceback))
    error_details = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))

    app = QApplication.instance()
    if app:
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Critical)
        msg.setWindowTitle("Unexpected Error")
        msg.setText(f"An unexpected error occurred in {APP_NAME}.")
        msg.setDetailedText(error_details)
        msg.setStandardButtons(QMessageBox.StandardButton.Ok)
        msg.exec()
    else:
        print("CRITICAL ERROR (No UI):", error_details)

class MediaType(Enum):
    AUDIO = "Audio"
    VIDEO = "Video"

class MediaFormat:
    MP3 = "mp3"
    FLAC = "flac"
    WAV = "wav"
    AAC = "aac"
    OPUS = "opus"
    MP4 = "mp4"
    MKV = "mkv"
    WEBM = "webm"
    AVI = "avi"
    MOV = "mov"
    WMV = "wmv"
    DIVX = "divx"

    @staticmethod
    def get_formats_for_type(media_type: MediaType) -> List[str]:
        if media_type == MediaType.AUDIO:
            return [MediaFormat.MP3, MediaFormat.FLAC, MediaFormat.WAV, MediaFormat.AAC, MediaFormat.OPUS]
        return [
            MediaFormat.MP4, MediaFormat.MKV, MediaFormat.WEBM, 
            MediaFormat.AVI, MediaFormat.MOV, MediaFormat.WMV, 
            MediaFormat.DIVX
        ]

    @staticmethod
    def get_quality_options(media_type: MediaType, fmt: str) -> List[str]:
        if media_type == MediaType.AUDIO:
            if fmt in [MediaFormat.MP3, MediaFormat.AAC]:
                return ["320", "256", "192", "128"]
            if fmt == MediaFormat.OPUS:
                return ["160", "128", "96"]
            return ["Lossless"]
        return ["Best Available", "4K (2160p)", "2K (1440p)", "Full HD (1080p)", "HD (720p)", "480p"]

@dataclass
class DownloadJobConfig:
    id: str
    url: str
    output_path: str
    media_type: MediaType
    format: str
    quality: str
    normalize: bool
    embed_metadata: bool
    filename_template: str

class DownloadSignals(QObject):
    started = pyqtSignal(str)              # job_id
    progress = pyqtSignal(str, float, str) # job_id, percent, speed
    finished = pyqtSignal(str)             # job_id
    error = pyqtSignal(str, str)           # job_id, error_msg
    status_update = pyqtSignal(str, str)   # job_id, status_msg
    metadata_ready = pyqtSignal(str, str)  # job_id, title

class DownloadWorker(QThread):
    def __init__(self, config: DownloadJobConfig):
        super().__init__()
        self.config = config
        self.signals = DownloadSignals()
        self._is_cancelled = False
        logging.debug(f"Worker initialized. Job ID: {config.id} | URL: {config.url}")

    def run(self) -> None:
        ydl_opts = self._build_options()
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                self.signals.status_update.emit(self.config.id, "Fetching metadata...")
                logging.info(f"[{self.config.id}] Extracting info...")
                
                info = ydl.extract_info(self.config.url, download=False)
                if not info:
                    raise ValueError("Could not fetch video metadata.")
                
                title = info.get('title', 'Unknown Title')
                self.signals.metadata_ready.emit(self.config.id, title)
                self.signals.started.emit(self.config.id)

                if self._is_cancelled: 
                    logging.info(f"[{self.config.id}] Cancelled before download.")
                    return

                logging.info(f"[{self.config.id}] Starting download: {title}")
                ydl.download([self.config.url])
                
            logging.info(f"[{self.config.id}] Completed.")
            self.signals.finished.emit(self.config.id)
            
        except Exception as e:
            logging.error(f"[{self.config.id}] Failed: {str(e)}", exc_info=True)
            self.signals.error.emit(self.config.id, str(e))

    def cancel(self):
        self._is_cancelled = True
        logging.warning(f"[{self.config.id}] Cancellation requested.")

    def _progress_hook(self, d: Dict[str, Any]) -> None:
        if self._is_cancelled:
            raise yt_dlp.utils.DownloadError("Download cancelled by user.")

        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate')
            downloaded = d.get('downloaded_bytes', 0)
            percent = (downloaded / total * 100) if total else 0.0
            speed = d.get('_speed_str', 'N/A')
            
            self.signals.progress.emit(self.config.id, percent, speed)
            self.signals.status_update.emit(self.config.id, "Downloading...")

        elif d['status'] == 'finished':
            self.signals.progress.emit(self.config.id, 100.0, "Processing")
            self.signals.status_update.emit(self.config.id, "Encoding/Merging...")

    def _get_video_format_string(self, quality_label: str, target_ext: str) -> str:
        height_map = {
            "4K (2160p)": 2160, "2K (1440p)": 1440,
            "Full HD (1080p)": 1080, "HD (720p)": 720, "480p": 480
        }
        h_val = height_map.get(quality_label, 1080)
        
        if quality_label == "Best Available":
            if target_ext == 'mp4':
                return f"bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            return "bestvideo+bestaudio/best"

        if target_ext == 'mp4':
            return (
                f"bestvideo[height<={h_val}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={h_val}]+bestaudio/"
                f"best[height<={h_val}]"
            )
        return f"bestvideo[height<={h_val}]+bestaudio/best[height<={h_val}]"

    def _build_options(self) -> Dict[str, Any]:
        out_path = Path(self.config.output_path)
        out_tmpl = str(out_path / self.config.filename_template)
        
        opts: Dict[str, Any] = {
            'outtmpl': out_tmpl,
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
            'writethumbnail': self.config.embed_metadata, 
            'addmetadata': self.config.embed_metadata,
            'parse_metadata': [
                '%(title)s:%(artist)s - %(title)s'
            ],
            
            'postprocessors': [], 
            'postprocessor_args': {},
            
            'http_headers': {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            },
            'extractor_args': {
                'youtube': {
                    'player_client': ['android', 'web']
                }
            }
        }

        if self.config.media_type == MediaType.AUDIO:
            opts['format'] = 'bestaudio/best'
            opts['postprocessors'].append({
                'key': 'FFmpegExtractAudio',
                'preferredcodec': self.config.format,
            })
            
            if self.config.format in [MediaFormat.MP3, MediaFormat.AAC, MediaFormat.OPUS]:
                opts['postprocessors'][-1]['preferredquality'] = self.config.quality
            
            if self.config.normalize:
                opts['postprocessor_args']['FFmpegExtractAudio'] = ['-af', 'loudnorm=I=-16:TP=-1.5:LRA=11']

        elif self.config.media_type == MediaType.VIDEO:
            target_fmt = self.config.format
            native_containers = [MediaFormat.MP4, MediaFormat.MKV, MediaFormat.WEBM]
            download_fmt_ext = target_fmt if target_fmt in native_containers else 'mp4' 
            
            opts['format'] = self._get_video_format_string(self.config.quality, download_fmt_ext)
            
            if target_fmt == MediaFormat.DIVX:
                opts['postprocessors'].append({
                    'key': 'FFmpegVideoConvertor',
                    'preferedformat': 'avi',
                })
                opts['postprocessor_args']['FFmpegVideoConvertor'] = [
                    '-c:v', 'mpeg4', '-vtag', 'DIVX', '-qscale:v', '3',
                    '-c:a', 'libmp3lame', '-qscale:a', '3'
                ]
            elif target_fmt not in native_containers:
                opts['postprocessors'].append({
                    'key': 'FFmpegVideoConvertor',
                    'preferedformat': target_fmt,
                })
            else:
                opts['merge_output_format'] = target_fmt
                if target_fmt == MediaFormat.MP4:
                    opts['postprocessor_args']['Merger'] = ['-c:v', 'copy', '-c:a', 'aac']

        if self.config.embed_metadata:
             opts['postprocessors'].append({'key': 'EmbedThumbnail'})
             
             opts['postprocessors'].append({
                 'key': 'FFmpegMetadata',
                 'add_chapters': True,
                 'add_metadata': True
             })
             
             opts['postprocessor_args']['FFmpegMetadata'] = [
                 '-metadata', f'comment=Downloaded via {APP_NAME}',
                 '-metadata', f'encoded_by={APP_NAME}'
             ]

        return opts

class ModernStyle:
    DARK_THEME = """
    QMainWindow { background-color: #1e1e1e; }
    QWidget { color: #f0f0f0; font-family: 'Segoe UI', 'Roboto', sans-serif; font-size: 14px; }
    
    QFrame#ControlPanel {
        background-color: #252526;
        border-radius: 8px;
        border: 1px solid #333;
    }
    
    QLineEdit, QComboBox {
        background-color: #2d2d2d;
        border: 1px solid #3d3d3d;
        border-radius: 4px;
        padding: 8px;
        selection-background-color: #007acc;
        min-height: 20px;
    }
    QLineEdit:focus, QComboBox:focus { border: 1px solid #007acc; }
    
    QPushButton {
        background-color: #007acc;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 10px 20px;
        font-weight: bold;
        min-width: 80px;
    }
    QPushButton:hover { background-color: #0098ff; }
    QPushButton:pressed { background-color: #005c99; }
    
    QPushButton#SecondaryBtn {
        background-color: #3d3d3d;
        padding: 8px 12px;
    }
    QPushButton#SecondaryBtn:hover { background-color: #4d4d4d; }
    
    QTableWidget {
        background-color: #252526;
        gridline-color: #3d3d3d;
        border: 1px solid #333;
        selection-background-color: #3d3d3d;
        outline: none;
    }
    QHeaderView::section {
        background-color: #1e1e1e;
        padding: 8px;
        border: none;
        border-bottom: 2px solid #3d3d3d;
        font-weight: bold;
    }
    QProgressBar {
        border: 1px solid #3d3d3d;
        border-radius: 4px;
        text-align: center;
        background-color: #2d2d2d;
        color: white;
    }
    QProgressBar::chunk { background-color: #007acc; border-radius: 3px; }
    """

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} v{VERSION}")
        self.resize(1100, 700)
        self.setMinimumSize(900, 500)
        
        self.active_downloads: Dict[str, DownloadWorker] = {}
        
        self._init_ui()
        self._apply_styles()
        logging.info("MainWindow initialized successfully.")

    def _init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(24)
        main_layout.setContentsMargins(24, 24, 24, 24)

        header_lbl = QLabel(APP_NAME)
        header_lbl.setStyleSheet("font-size: 28px; font-weight: bold; color: #007acc; letter-spacing: 1px;")
        main_layout.addWidget(header_lbl)

        control_panel = QFrame()
        control_panel.setObjectName("ControlPanel")
        
        grid = QGridLayout(control_panel)
        grid.setVerticalSpacing(16)
        grid.setHorizontalSpacing(16)
        grid.setContentsMargins(20, 20, 20, 20)

        lbl_url = QLabel("Media URL:")
        lbl_url.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Preferred)
        self.url_input = QLineEdit()
        self.url_input.setPlaceholderText("Paste YouTube, Vimeo, or SoundCloud link...")
        btn_paste = QPushButton("Paste")
        btn_paste.setObjectName("SecondaryBtn")
        btn_paste.clicked.connect(self._paste_from_clipboard)

        grid.addWidget(lbl_url, 0, 0)
        grid.addWidget(self.url_input, 0, 1, 1, 3)
        grid.addWidget(btn_paste, 0, 4)

        grid.addWidget(QLabel("Download Type:"), 1, 0)
        self.type_combo = QComboBox()
        self.type_combo.addItems([MediaType.AUDIO.value, MediaType.VIDEO.value])
        self.type_combo.currentTextChanged.connect(self._on_type_changed)
        grid.addWidget(self.type_combo, 1, 1)

        grid.addWidget(QLabel("Save Location:"), 1, 2)
        path_layout = QHBoxLayout()
        path_layout.setSpacing(8)
        self.path_input = QLineEdit(DEFAULT_DOWNLOAD_DIR)
        self.path_input.setReadOnly(True)
        btn_browse = QPushButton("Browse")
        btn_browse.setObjectName("SecondaryBtn")
        btn_browse.clicked.connect(self._browse_directory)
        path_layout.addWidget(self.path_input)
        path_layout.addWidget(btn_browse)
        
        path_container = QWidget()
        path_container.setLayout(path_layout)
        path_layout.setContentsMargins(0,0,0,0)
        grid.addWidget(path_container, 1, 3, 1, 2)

        grid.addWidget(QLabel("Format:"), 2, 0)
        self.fmt_combo = QComboBox()
        self.fmt_combo.currentTextChanged.connect(self._update_quality_options)
        grid.addWidget(self.fmt_combo, 2, 1)

        grid.addWidget(QLabel("Quality:"), 2, 2)
        self.quality_combo = QComboBox()
        self.quality_combo.setMinimumWidth(150)
        grid.addWidget(self.quality_combo, 2, 3)

        self.chk_metadata = QComboBox()
        self.chk_metadata.addItems(["Embed Metadata + Cover", "No Metadata"])
        grid.addWidget(self.chk_metadata, 2, 4)

        self.btn_download = QPushButton("Start Download")
        self.btn_download.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_download.setFixedHeight(45)
        self.btn_download.clicked.connect(self._start_download)
        grid.addWidget(self.btn_download, 3, 0, 1, 5)

        main_layout.addWidget(control_panel)

        self.table = QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Title / Description", "Type", "Status", "Progress", "Controls"])
        
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)
        
        self.table.setColumnWidth(1, 80)
        self.table.setColumnWidth(2, 180)
        self.table.setColumnWidth(3, 200)
        self.table.setColumnWidth(4, 110) 
        
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.table.setSortingEnabled(False)
        
        main_layout.addWidget(self.table)

        self.status_bar = QLabel("System Ready")
        self.status_bar.setStyleSheet("color: #666; font-size: 12px;")
        main_layout.addWidget(self.status_bar)

        self._on_type_changed(MediaType.AUDIO.value)

    def _apply_styles(self):
        self.setStyleSheet(ModernStyle.DARK_THEME)

    def _on_type_changed(self, type_str: str):
        media_type = MediaType(type_str)
        self.fmt_combo.blockSignals(True)
        self.fmt_combo.clear()
        self.fmt_combo.addItems(MediaFormat.get_formats_for_type(media_type))
        self.fmt_combo.blockSignals(False)
        self._update_quality_options(self.fmt_combo.currentText())

    def _update_quality_options(self, fmt: str):
        if not fmt: return
        media_type = MediaType(self.type_combo.currentText())
        self.quality_combo.clear()
        opts = MediaFormat.get_quality_options(media_type, fmt)
        self.quality_combo.addItems(opts)

    def _browse_directory(self):
        d = QFileDialog.getExistingDirectory(self, "Select Output Directory", self.path_input.text())
        if d: self.path_input.setText(d)

    def _paste_from_clipboard(self):
        self.url_input.setText(QApplication.clipboard().text())

    def _start_download(self):
        try:
            url = self.url_input.text().strip()
            if not url:
                QMessageBox.warning(self, "Validation Error", "Please input a valid URL.")
                return

            job_id = str(uuid.uuid4())

            config = DownloadJobConfig(
                id=job_id,
                url=url,
                output_path=self.path_input.text(),
                media_type=MediaType(self.type_combo.currentText()),
                format=self.fmt_combo.currentText(),
                quality=self.quality_combo.currentText(),
                normalize=True,
                embed_metadata=self.chk_metadata.currentIndex() == 0,
                filename_template="%(title)s.%(ext)s"
            )

            worker = DownloadWorker(config)
            worker.signals.metadata_ready.connect(self._on_metadata_ready)
            worker.signals.progress.connect(self._on_progress)
            worker.signals.finished.connect(self._on_finished)
            worker.signals.error.connect(self._on_error)
            worker.signals.status_update.connect(self._on_status)

            self._add_table_row(url, config)
            self.active_downloads[job_id] = worker
            worker.start()
            self.url_input.clear()
            
        except Exception as e:
            logging.error("Failed to start download job", exc_info=True)
            QMessageBox.critical(self, "Task Error", f"Could not start download: {str(e)}")

    def _add_table_row(self, url: str, config: DownloadJobConfig):
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        item_title = QTableWidgetItem(url)
        item_title.setData(Qt.ItemDataRole.UserRole, config.id) 
        
        item_type = QTableWidgetItem(config.media_type.value)
        item_type.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        
        item_status = QTableWidgetItem("Initializing...")
        
        p_bar = QProgressBar()
        p_bar.setValue(0)
        p_bar.setTextVisible(False)
        
        btn_cancel = QPushButton("Cancel")
        btn_cancel.setObjectName("SecondaryBtn")
        btn_cancel.setStyleSheet("padding: 4px 12px;")
        btn_cancel.clicked.connect(lambda: self._cancel_job(config.id))
        
        w_btn = QWidget()
        l_btn = QHBoxLayout(w_btn)
        l_btn.setContentsMargins(0,0,0,0)
        l_btn.setAlignment(Qt.AlignmentFlag.AlignCenter)
        l_btn.addWidget(btn_cancel)

        for item in [item_title, item_type, item_status]:
            item.setFlags(item.flags() ^ Qt.ItemFlag.ItemIsEditable)

        self.table.setItem(row, 0, item_title)
        self.table.setItem(row, 1, item_type)
        self.table.setItem(row, 2, item_status)
        self.table.setCellWidget(row, 3, p_bar)
        self.table.setCellWidget(row, 4, w_btn)
        self.table.scrollToBottom()

    def _get_row(self, job_id: str) -> int:
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item and item.data(Qt.ItemDataRole.UserRole) == job_id:
                return r
        return -1

    def _on_metadata_ready(self, job_id: str, title: str):
        row = self._get_row(job_id)
        if row >= 0:
            self.table.item(row, 0).setText(title)

    def _on_progress(self, job_id: str, percent: float, speed: str):
        row = self._get_row(job_id)
        if row >= 0:
            pb: QProgressBar = self.table.cellWidget(row, 3) # type: ignore
            if pb: pb.setValue(int(percent))
            self.table.item(row, 2).setText(f"▼ {speed}")

    def _on_status(self, job_id: str, msg: str):
        row = self._get_row(job_id)
        if row >= 0: self.table.item(row, 2).setText(msg)

    def _on_finished(self, job_id: str):
        row = self._get_row(job_id)
        if row >= 0:
            self.table.item(row, 2).setText("✔ Success")
            self.table.item(row, 2).setForeground(QColor("#4caf50"))
            self.table.cellWidget(row, 3).setValue(100) # type: ignore
            self.table.setCellWidget(row, 4, None)
        self._cleanup(job_id)

    def _on_error(self, job_id: str, err: str):
        row = self._get_row(job_id)
        if row >= 0:
            self.table.item(row, 2).setText("✘ Error")
            self.table.item(row, 2).setForeground(QColor("#f44336"))
            self.table.setCellWidget(row, 4, None)
            logging.error(f"UI error signal for Job {job_id}: {err}")
        self._cleanup(job_id)

    def _cancel_job(self, job_id: str):
        if job_id in self.active_downloads:
            self.active_downloads[job_id].cancel()
            self.status_bar.setText(f"Cancelling Job: {job_id}")

    def _cleanup(self, job_id: str):
        if job_id in self.active_downloads:
            self.active_downloads[job_id].quit()
            self.active_downloads[job_id].wait()
            del self.active_downloads[job_id]
            self.status_bar.setText("System Ready")

def main():
    setup_logging()
    sys.excepthook = global_exception_hook
    
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    if hasattr(Qt.ApplicationAttribute, 'AA_EnableHighDpiScaling'):
        app.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
    if hasattr(Qt.ApplicationAttribute, 'AA_UseHighDpiPixmaps'):
        app.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
    
    window = MainWindow()
    window.show()
    
    exit_code = app.exec()
    logging.info(f"Application closing with code: {exit_code}")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()