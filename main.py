import sys
import logging
import uuid
import re
import types
from typing import Final, Dict, Any, Optional, cast
from pathlib import Path

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QMessageBox, QFrame, QScrollArea, QGroupBox, QFormLayout, 
    QCheckBox, QSpinBox, QPlainTextEdit,
    QSplitter, QTabWidget, QRadioButton, QButtonGroup, QAbstractItemView, QMenu
)
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThreadPool, pyqtSlot, QUrl
from PyQt6.QtGui import QColor, QPixmap, QFont, QTextCursor, QTextCharFormat, QDesktopServices, QPalette, QAction, QCloseEvent

import processamento as proc

APP_NAME: Final[str] = "SoundStream Pro"
VERSION: Final[str] = "7.1.0"
DEFAULT_DOWNLOAD_DIR: Final[Path] = Path.home() / "Downloads"
MAX_CONCURRENT_DOWNLOADS: Final[int] = 3


class ThemeManager:
    @staticmethod
    def get_palette(is_dark: bool) -> QPalette:
        palette = QPalette()
        if is_dark:
            palette.setColor(QPalette.ColorRole.Window, QColor(18, 18, 18))
            palette.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.white)
            palette.setColor(QPalette.ColorRole.Base, QColor(30, 30, 30))
            palette.setColor(QPalette.ColorRole.AlternateBase, QColor(40, 40, 40))
            palette.setColor(QPalette.ColorRole.ToolTipBase, Qt.GlobalColor.black)
            palette.setColor(QPalette.ColorRole.ToolTipText, Qt.GlobalColor.white)
            palette.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.white)
            palette.setColor(QPalette.ColorRole.Button, QColor(53, 53, 53))
            palette.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.white)
            palette.setColor(QPalette.ColorRole.Highlight, QColor(0, 122, 204))
            palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.white)
            
            palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(120, 120, 120))
            palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, QColor(120, 120, 120))
            palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(120, 120, 120))
        else:
            palette.setColor(QPalette.ColorRole.Window, QColor(245, 245, 245))
            palette.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.black)
            palette.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
            palette.setColor(QPalette.ColorRole.AlternateBase, QColor(240, 240, 240))
            palette.setColor(QPalette.ColorRole.ToolTipBase, Qt.GlobalColor.white)
            palette.setColor(QPalette.ColorRole.ToolTipText, Qt.GlobalColor.black)
            palette.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.black)
            palette.setColor(QPalette.ColorRole.Button, QColor(225, 225, 225))
            palette.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.black)
            palette.setColor(QPalette.ColorRole.Highlight, QColor(0, 122, 204))
            palette.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.white)
            
            palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Text, QColor(150, 150, 150))
            palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.WindowText, QColor(150, 150, 150))
            palette.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(150, 150, 150))
        return palette

    @staticmethod
    def get_stylesheet(is_dark: bool) -> str:
        qss = """
            QWidget { font-family: 'Segoe UI', 'Roboto', sans-serif; font-size: 13px; }
            QFrame#Panel { border-radius: 8px; border: 1px solid UID_BORDER; background-color: UID_PANEL_BG; }
            QPushButton#PrimaryAction { background-color: #007acc; color: white; border-radius: 4px; padding: 8px 16px; font-weight: bold; border: none; }
            QPushButton#PrimaryAction:hover { background-color: #0062a3; }
            QPushButton#PrimaryAction:disabled { background-color: UID_DISABLED_BG; color: UID_DISABLED_TXT; }
            QPushButton#Destructive { color: #d32f2f; border: 1px solid #d32f2f; border-radius: 4px; padding: 4px 8px; background-color: transparent; }
            QPushButton#Destructive:hover { background-color: rgba(211, 47, 47, 0.1); }
            QLabel#MainHeader { color: #007acc; font-size: 24px; font-weight: 800; letter-spacing: 2px; }
            QLabel#ThumbLabel { background-color: UID_THUMB_BG; border: 1px solid UID_BORDER; color: UID_THUMB_TEXT; }
            QLabel#StatsLabel, QLabel#LogHeader { color: UID_THUMB_TEXT; }
            QLabel#LogHeader { font-weight: bold; font-size: 10px; letter-spacing: 1px; }
            QPlainTextEdit#LogConsole { background-color: UID_CONSOLE_BG; color: UID_CONSOLE_TEXT; font-family: 'Consolas', 'Courier New', monospace; font-size: 11px; border: 1px solid UID_BORDER; border-radius: 4px; }
            QMenu { background-color: UID_PANEL_BG; border: 1px solid UID_BORDER; padding: 5px; }
            QMenu::item { padding: 6px 20px 6px 20px; border-radius: 4px; }
            QMenu::item:selected { background-color: #007acc; color: white; }
            QMenu::separator { height: 1px; background: UID_BORDER; margin: 4px 10px; }
        """
        if is_dark:
            return qss.replace("UID_BORDER", "#3d3d3d").replace("UID_PANEL_BG", "#1e1e1e").replace("UID_THUMB_BG", "#000000").replace("UID_THUMB_TEXT", "#aaaaaa").replace("UID_CONSOLE_BG", "#0e0e0e").replace("UID_CONSOLE_TEXT", "#d4d4d4").replace("UID_DISABLED_BG", "#333333").replace("UID_DISABLED_TXT", "#777777")
        return qss.replace("UID_BORDER", "#cccccc").replace("UID_PANEL_BG", "#ffffff").replace("UID_THUMB_BG", "#eaeaea").replace("UID_THUMB_TEXT", "#666666").replace("UID_CONSOLE_BG", "#ffffff").replace("UID_CONSOLE_TEXT", "#333333").replace("UID_DISABLED_BG", "#e0e0e0").replace("UID_DISABLED_TXT", "#999999")

class QtLogHandler(logging.Handler, QObject):
    log_record = pyqtSignal(str, int)

    def __init__(self) -> None:
        logging.Handler.__init__(self)
        QObject.__init__(self)
        self.setFormatter(logging.Formatter('[%(asctime)s] %(name)s: %(message)s', datefmt='%H:%M:%S'))

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        self.log_record.emit(msg, record.levelno)

class LogViewerWidget(QWidget):
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.fmt_debug = QTextCharFormat()
        self.fmt_info = QTextCharFormat()
        self.fmt_warning = QTextCharFormat()
        self.fmt_error = QTextCharFormat()
        self.fmt_critical = QTextCharFormat()
        self.fmt_critical.setFontWeight(QFont.Weight.Bold)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        header_layout = QHBoxLayout()
        lbl = QLabel("SYSTEM LOGS / DEBUG CONSOLE")
        lbl.setObjectName("LogHeader")
        
        btn_clear = QPushButton("Clear")
        btn_clear.setFixedSize(60, 24)
        btn_clear.clicked.connect(self.clear_logs)
        
        header_layout.addWidget(lbl)
        header_layout.addStretch()
        header_layout.addWidget(btn_clear)
        
        self.text_edit = QPlainTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setObjectName("LogConsole")
        self.text_edit.setMaximumBlockCount(2000) 
        
        layout.addLayout(header_layout)
        layout.addWidget(self.text_edit)

    def update_theme_colors(self, is_dark: bool) -> None:
        if is_dark:
            self.fmt_debug.setForeground(QColor("#808080"))
            self.fmt_info.setForeground(QColor("#569cd6"))
            self.fmt_warning.setForeground(QColor("#dcdcaa"))
            self.fmt_error.setForeground(QColor("#f44747"))
            self.fmt_critical.setForeground(QColor("#ff0000"))
        else:
            self.fmt_debug.setForeground(QColor("#666666"))
            self.fmt_info.setForeground(QColor("#005a9e"))  
            self.fmt_warning.setForeground(QColor("#b8860b")) 
            self.fmt_error.setForeground(QColor("#d32f2f"))  
            self.fmt_critical.setForeground(QColor("#b71c1c"))

    @pyqtSlot(str, int)
    def append_log(self, msg: str, levelno: int) -> None:
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

    def clear_logs(self) -> None:
        self.text_edit.clear()

class InspectorPanel(QFrame):
    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setObjectName("Panel")
        self._current_meta: Optional[proc.MediaMetadata] = None
        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

        left_col = QWidget()
        left_layout = QVBoxLayout(left_col)
        
        self.thumb_lbl = QLabel("No Preview")
        self.thumb_lbl.setFixedSize(320, 180)
        self.thumb_lbl.setObjectName("ThumbLabel")
        self.thumb_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.thumb_lbl.setScaledContents(True)
        
        self.stats_lbl = QLabel("Ready to Analyze")
        self.stats_lbl.setObjectName("StatsLabel")
        self.stats_lbl.setWordWrap(True)
        
        left_layout.addWidget(self.thumb_lbl)
        left_layout.addWidget(self.stats_lbl)
        left_layout.addStretch()

        right_container = QWidget()
        right_layout = QVBoxLayout(right_container)
        
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
        
        self.tabs = QTabWidget()
        
        tab_format = QWidget()
        fmt_layout = QVBoxLayout(tab_format)
        fmt_grid = QGridLayout()
        
        self.cb_container = QComboBox()
        self.cb_container.currentTextChanged.connect(self._on_container_changed)
        
        self.cb_abitrate = QComboBox()
        for k in ["320", "256", "192", "128", "96"]:
            self.cb_abitrate.addItem(f"{k} kbps", k)
            
        self.sb_asr = QSpinBox()
        self.sb_asr.setRange(0, 192000)
        self.sb_asr.setSpecialValueText("Auto")
        self.sb_asr.setSuffix(" Hz")
        
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

    def set_metadata(self, meta: proc.MediaMetadata):
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

    def set_thumbnail(self, pixmap: QPixmap) -> None:
        self.thumb_lbl.setPixmap(pixmap)

    def _update_ui_mode(self) -> None:
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
            'media_type': proc.MediaType.VIDEO if self.rb_video.isChecked() else proc.MediaType.AUDIO,
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

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {VERSION}")
        self.resize(1200, 900)
        self.setMinimumSize(1000, 700)
        
        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(MAX_CONCURRENT_DOWNLOADS)
        self.active_runnables: Dict[str, proc.DownloadRunnable] = {}
        self._current_meta: Optional[proc.MediaMetadata] = None
        
        self.qt_log_handler = QtLogHandler()
        logging.getLogger().addHandler(self.qt_log_handler)
        
        self.init_ui()
        self._init_menu_bar()
        self._apply_theme(is_dark=True) 

    def _init_menu_bar(self) -> None:
        menu_bar = self.menuBar()
        
        menu_file = menu_bar.addMenu("Arquivo")
        action_exit = QAction("Sair", self)
        action_exit.triggered.connect(self.close)
        menu_file.addAction(action_exit)

        menu_view = menu_bar.addMenu("Exibir")
        menu_theme = menu_view.addMenu("Tema")
        
        action_theme_light = QAction("Claro", self)
        action_theme_light.triggered.connect(lambda: self._apply_theme(is_dark=False))
        menu_theme.addAction(action_theme_light)
        
        action_theme_dark = QAction("Escuro", self)
        action_theme_dark.triggered.connect(lambda: self._apply_theme(is_dark=True))
        menu_theme.addAction(action_theme_dark)
        
        menu_help = menu_bar.addMenu("Ajuda")
        action_about = QAction("Sobre", self)
        action_about.triggered.connect(self._show_about_dialog)
        menu_help.addAction(action_about)

    def _apply_theme(self, is_dark: bool) -> None:
        app = QApplication.instance()
        if app is not None:
            app.setPalette(ThemeManager.get_palette(is_dark))
            app.setStyleSheet(ThemeManager.get_stylesheet(is_dark))
            
        self.log_viewer.update_theme_colors(is_dark)

    def _show_about_dialog(self) -> None:
        QMessageBox.about(self, "Sobre", f"{APP_NAME} {VERSION}\n\nInterface construída via PyQt6 com arquitetura orientada a eventos.")

    def init_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        
        main_splitter = QSplitter(Qt.Orientation.Vertical)
        top_container = QWidget()
        main_layout = QVBoxLayout(top_container)
        main_layout.setContentsMargins(20, 20, 20, 0)
        
        top_bar = QHBoxLayout()
        header = QLabel(APP_NAME.upper())
        header.setObjectName("MainHeader")
        
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
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        header_view = self.table.horizontalHeader()
        header_view.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        
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

    def _show_context_menu(self, pos):
        item = self.table.itemAt(pos)
        menu = QMenu(self)
        
        if item:
            row = item.row()
            job_id = self.table.item(row, 0).data(Qt.ItemDataRole.UserRole)
            
            action_remove = QAction("Remove from Queue", self)
            action_remove.triggered.connect(lambda: self._remove_from_queue(job_id))
            menu.addAction(action_remove)
            
            if job_id in self.active_runnables:
                action_cancel = QAction("Cancel Download (Instant)", self)
                action_cancel.triggered.connect(lambda: self.cancel_job(job_id))
                menu.addAction(action_cancel)
                
            menu.addSeparator()
            
        action_open = QAction("Open Output Folder", self)
        action_open.triggered.connect(self.open_output_folder)
        menu.addAction(action_open)
        
        action_clear = QAction("Clear All Completed", self)
        action_clear.triggered.connect(self._clear_finished_jobs)
        menu.addAction(action_clear)
        
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def _remove_from_queue(self, job_id: str):
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            del self.active_runnables[job_id]
            
        row = self.get_row_by_id(job_id)
        if row >= 0:
            self.table.removeRow(row)

    def _clear_finished_jobs(self) -> None:
        for row in range(self.table.rowCount() - 1, -1, -1):
            status_item = self.table.item(row, 2)
            if status_item is not None and status_item.text() in ["✔ Done", "✘ Error", "Cancelled"]:
                self.table.removeRow(row)

    def toggle_dev_mode(self, checked: bool) -> None:
        logging.getLogger().setLevel(logging.DEBUG if checked else logging.INFO)
        self.log_viewer.setVisible(checked)

    def start_analysis(self) -> None:
        url = self.url_input.text().strip()
        if not url: return
        
        if not proc.YtDlpService.validate_url(url):
            QMessageBox.warning(self, "Invalid URL", "The provided URL pattern is not supported.\nPlease verify the link and try again.")
            return
        
        self.btn_analyze.setEnabled(False)
        self.btn_analyze.setText("Fetching...")
        worker = proc.AnalysisRunnable(url)
        worker.signals.result.connect(self.on_analysis_success)
        worker.signals.thumbnail_data.connect(self.on_thumbnail_ready)
        worker.signals.error.connect(self.on_analysis_error)
        worker.signals.finished.connect(lambda: self.btn_analyze.setEnabled(True))
        worker.signals.finished.connect(lambda: self.btn_analyze.setText("Analyze Media"))
        self.thread_pool.start(worker)

    @pyqtSlot(object)
    def on_analysis_success(self, meta: proc.MediaMetadata) -> None:
        self._current_meta = meta
        self.inspector.set_metadata(meta)
        self.inspector.setVisible(True)
        self.action_bar.setVisible(True)

    @pyqtSlot(bytes)
    def on_thumbnail_ready(self, data: bytes) -> None:
        pixmap = QPixmap()
        pixmap.loadFromData(data)
        self.inspector.set_thumbnail(pixmap)

    @pyqtSlot(str)
    def on_analysis_error(self, err_msg: str) -> None:
        QMessageBox.critical(self, "Analysis Failed", err_msg)
        self.inspector.setVisible(False)
        self.action_bar.setVisible(False)

    def browse_folder(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Save Location", self.path_input.text())
        if d: self.path_input.setText(d)

    def open_output_folder(self) -> None:
        path_str = self.path_input.text()
        if path_str:
            path_obj = Path(path_str)
            if path_obj.exists() and path_obj.is_dir():
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(path_obj)))
            else:
                QMessageBox.warning(self, "Warning", "The specified directory does not exist or cannot be accessed.")

    def queue_download(self) -> None:
        if not self._current_meta: return
        data = self.inspector.get_config_delta()
        
        config = proc.DownloadJobConfig(
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

    def _spawn_download(self, config: proc.DownloadJobConfig):
        runnable = proc.DownloadRunnable(config)
        runnable.signals.progress.connect(self.update_progress)
        runnable.signals.status.connect(self.update_status)
        runnable.signals.finished.connect(lambda: self.on_job_finished(config.job_id))
        runnable.signals.error.connect(lambda err: self.on_job_error(config.job_id, err))
        self.active_runnables[config.job_id] = runnable
        self.add_table_row(config)
        self.thread_pool.start(runnable)

    def add_table_row(self, config: proc.DownloadJobConfig) -> None:
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        display_name = f"{config.custom_filename}.{config.format_container}"
        title_item = QTableWidgetItem(display_name)
        title_item.setToolTip(config.meta_title)
        title_item.setData(Qt.ItemDataRole.UserRole, config.job_id)
        
        fmt_str = config.format_container.upper()
        if config.media_type == proc.MediaType.VIDEO:
            fmt_str += f" ({config.quality_preset})"
            
        pbar = QProgressBar()
        pbar.setValue(0)
        
        btn_layout = QHBoxLayout()
        btn_layout.setContentsMargins(0, 0, 0, 0)
        btn_container = QWidget()
        
        btn_cancel = QPushButton("Stop")
        btn_cancel.setObjectName("Destructive")
        btn_cancel.clicked.connect(lambda: self.cancel_job(config.job_id))
        
        btn_layout.addWidget(btn_cancel)
        btn_container.setLayout(btn_layout)
        
        self.table.setItem(row, 0, title_item)
        self.table.setItem(row, 1, QTableWidgetItem(fmt_str))
        self.table.setItem(row, 2, QTableWidgetItem("Queued"))
        self.table.setCellWidget(row, 3, pbar)
        self.table.setCellWidget(row, 4, btn_container)

    def get_row_by_id(self, job_id: str) -> int:
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item is not None and item.data(Qt.ItemDataRole.UserRole) == job_id: return r
        return -1

    @pyqtSlot(str, float, str)
    def update_progress(self, job_id: str, pct: float, speed: str) -> None:
        row = self.get_row_by_id(job_id)
        if row >= 0:
            widget = self.table.cellWidget(row, 3)
            if isinstance(widget, QProgressBar):
                widget.setValue(int(pct))
            
            item = self.table.item(row, 2)
            if item is not None:
                item.setText(f"▼ {speed}")

    @pyqtSlot(str, str)
    def update_status(self, job_id: str, msg: str) -> None:
        row = self.get_row_by_id(job_id)
        if row >= 0:
            item = self.table.item(row, 2)
            if item is not None:
                item.setText(msg)

    def on_job_finished(self, job_id: str) -> None:
        self._cleanup_job(job_id, "✔ Done", QColor("#4caf50"))

    def on_job_error(self, job_id: str, err: str) -> None:
        self._cleanup_job(job_id, "✘ Error", QColor("#d32f2f"))

    def cancel_job(self, job_id: str) -> None:
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            self._cleanup_job(job_id, "Cancelled", QColor("#ff9800"))

    def _cleanup_job(self, job_id: str, status_text: str, color: QColor) -> None:
        row = self.get_row_by_id(job_id)
        if row >= 0:
            item = self.table.item(row, 2)
            if item is not None:
                item.setText(status_text)
                item.setForeground(color)
            
            if "Done" in status_text: 
                widget = self.table.cellWidget(row, 3)
                if isinstance(widget, QProgressBar):
                    widget.setValue(100)
                
                btn_open = QPushButton("Open Folder")
                btn_open.clicked.connect(self.open_output_folder)
                
                layout = QHBoxLayout()
                layout.setContentsMargins(0, 0, 0, 0)
                layout.addWidget(btn_open)
                container = QWidget()
                container.setLayout(layout)
                self.table.setCellWidget(row, 4, container)
                
            elif "Cancelled" in status_text:
                self.table.cellWidget(row, 3).setValue(0)
                self.table.setCellWidget(row, 4, None)
                
            elif "Error" in status_text:
                self.table.setCellWidget(row, 4, None)
                
        if job_id in self.active_runnables: 
            del self.active_runnables[job_id]

    def closeEvent(self, event: QCloseEvent) -> None:
        root_logger = logging.getLogger()
        if self.qt_log_handler in root_logger.handlers:
            root_logger.removeHandler(self.qt_log_handler)
            self.qt_log_handler.close()
        
        self.thread_pool.clear()
        event.accept()

def handle_exception(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: Optional[types.TracebackType]) -> None:
    logging.critical("Uncaught Exception:", exc_info=(exc_type, exc_value, exc_traceback))

def main() -> None:
    sys.excepthook = handle_exception
    logging.basicConfig(level=logging.INFO)
    
    app = QApplication(sys.argv)
    app.setStyle("Fusion") 
    
    win = MainWindow()
    win.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()