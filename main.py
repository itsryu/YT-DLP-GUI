import sys
import logging
import uuid
import re
import types
import json
import urllib.parse
import urllib.request
import urllib.error
import time
import ssl
from dataclasses import dataclass
from typing import Final, Dict, Any, Optional, List
from pathlib import Path

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QMessageBox, QFrame, QScrollArea, QGroupBox, QFormLayout, 
    QCheckBox, QPlainTextEdit, QSplitter, QTabWidget, QRadioButton, 
    QButtonGroup, QAbstractItemView, QMenu, QDialog, QDialogButtonBox
)
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThreadPool, pyqtSlot, QUrl, QRunnable
from PyQt6.QtGui import QColor, QPixmap, QFont, QTextCursor, QTextCharFormat, QDesktopServices, QPalette, QAction, QCloseEvent, QImage
from PyQt6 import sip

# Pressupõe-se a existência do módulo de processamento no ambiente
import processamento as proc

APP_NAME: Final[str] = "SoundStream Pro"
VERSION: Final[str] = "7.3.0"
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
        lbl = QLabel("REGISTOS DO SISTEMA / CONSOLA DE DEPURAÇÃO", self)
        lbl.setObjectName("LogHeader")
        
        btn_clear = QPushButton("Limpar", self)
        btn_clear.setFixedSize(60, 24)
        btn_clear.clicked.connect(self.clear_logs)
        
        header_layout.addWidget(lbl)
        header_layout.addStretch()
        header_layout.addWidget(btn_clear)
        
        self.text_edit = QPlainTextEdit(self)
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

@dataclass
class MetadataCandidate:
    title: str
    artist: str
    album: str
    date: str
    genre: str
    release_id: str  # MBID necessário para o Cover Art Archive

class MusicBrainzSignals(QObject):
    results_ready = pyqtSignal(list)
    error = pyqtSignal(str)

class CoverArtSignals(QObject):
    result_ready = pyqtSignal(QImage)
    error = pyqtSignal(str)

class MusicBrainzWorker(QRunnable):
    def __init__(self, title_query: str, artist_query: str, album_query: str, date_query: str, app_name: str, version: str) -> None:
        super().__init__()
        self.title_query = title_query
        self.artist_query = artist_query
        self.album_query = album_query
        self.date_query = date_query
        self.app_name = app_name
        self.version = version
        self.signals = MusicBrainzSignals()

    def _escape_lucene(self, text: str) -> str:
        """Limpa a string para compatibilidade com a sintaxe de pesquisa Lucene."""
        return re.sub(r'([+\-!(){}\[\]^"~*?:\\])', r'\\\1', text)

    @pyqtSlot()
    def run(self) -> None:
        try:
            query_parts = []
            if self.title_query:
                query_parts.append(f'recording:"{self._escape_lucene(self.title_query)}"')
            if self.artist_query:
                query_parts.append(f'artist:"{self._escape_lucene(self.artist_query)}"')
            if self.album_query:
                query_parts.append(f'release:"{self._escape_lucene(self.album_query)}"')
            if self.date_query:
                # O MusicBrainz aceita anos (YYYY) no campo date da entidade recording
                query_parts.append(f'date:"{self._escape_lucene(self.date_query)}"')
            
            if not query_parts:
                self.signals.results_ready.emit([])
                return

            query = " AND ".join(query_parts)
            encoded_query = urllib.parse.quote(query)
            
            url = f"https://musicbrainz.org/ws/2/recording/?query={encoded_query}&fmt=json"
            
            headers = {
                "User-Agent": f"{self.app_name}/{self.version} ( dev@localhost )",
                "Accept": "application/json",
                "Connection": "close"
            }
            req = urllib.request.Request(url, headers=headers)
            ctx = ssl.create_default_context()
            
            max_retries = 3
            data = None
            
            for attempt in range(max_retries):
                try:
                    with urllib.request.urlopen(req, timeout=15, context=ctx) as response:
                        data = json.loads(response.read().decode('utf-8'))
                    break
                except (urllib.error.URLError, ssl.SSLError, ConnectionError) as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(2 ** attempt)
            
            if not data:
                self.signals.results_ready.emit([])
                return
                
            candidates: List[MetadataCandidate] = []
            for rec in data.get("recordings", [])[:15]: 
                title = rec.get("title", "")
                
                artist_credits = rec.get("artist-credit", [])
                artist = "".join([ac.get("name", "") + ac.get("joinphrase", "") for ac in artist_credits])
                
                releases = rec.get("releases", [])
                album = releases[0].get("title", "") if releases else ""
                date = releases[0].get("date", "")[:4] if releases and releases[0].get("date") else ""
                
                # Resolução do identificador universal (MBID) para interoperabilidade com CAA
                release_id = releases[0].get("id", "") if releases else ""
                
                tags = rec.get("tags", [])
                genre = tags[0].get("name", "").title() if tags else ""
                
                candidates.append(MetadataCandidate(title, artist, album, date, genre, release_id))
                
            self.signals.results_ready.emit(candidates)
            
        except Exception as e:
            self.signals.error.emit(str(e))

class CoverArtWorker(QRunnable):
    """
    Worker especializado na obtenção de arte de capa na resolução original.
    Implementa resiliência TLS via Connection:close e retentativas com Exponential Backoff.
    """
    def __init__(self, release_id: str) -> None:
        super().__init__()
        self.release_id = release_id
        self.signals = CoverArtSignals()

    @pyqtSlot()
    def run(self) -> None:
        if not self.release_id:
            self.signals.error.emit("Sem MBID de Release.")
            return

        # O endpoint /front redireciona para a imagem original (máxima qualidade)
        url = f"https://coverartarchive.org/release/{self.release_id}/front"
        headers = {
            "User-Agent": f"{APP_NAME}/{VERSION} ( dev@localhost )",
            "Accept": "image/*",
            "Connection": "close"
        }
        req = urllib.request.Request(url, headers=headers)
        ctx = ssl.create_default_context()
        
        max_retries = 3
        buffer = None

        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(req, timeout=25, context=ctx) as response:
                    buffer = response.read()
                break
            except (urllib.error.URLError, ssl.SSLError, ConnectionError) as e:
                if attempt == max_retries - 1:
                    self.signals.error.emit(f"Falha persistente na camada SSL/TLS: {str(e)}")
                    return
                # Recuo exponencial para mitigar rate-limiting ou instabilidade de handshake
                time.sleep(2 ** attempt)

        if not buffer:
            return

        try:
            image = QImage()
            if image.loadFromData(buffer):
                self.signals.result_ready.emit(image)
            else:
                self.signals.error.emit("A descodificação do fluxo binário da imagem falhou.")
        except Exception as e:
            self.signals.error.emit(f"Erro de processamento de imagem: {str(e)}")

class MetadataSelectionDialog(QDialog):
    def __init__(self, candidates: List[MetadataCandidate], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("MusicBrainz: Selecionar Metadados")
        self.setMinimumSize(700, 400)
        self.selected_candidate: Optional[MetadataCandidate] = None
        self.candidates = candidates
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        
        self.table = QTableWidget(self)
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Título", "Artista", "Álbum", "Ano", "Género"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        
        header = self.table.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        
        self._populate_table()
        layout.addWidget(self.table)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel, self)
        btn_box.accepted.connect(self._on_accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def _populate_table(self) -> None:
        self.table.setRowCount(len(self.candidates))
        for row, cand in enumerate(self.candidates):
            self.table.setItem(row, 0, QTableWidgetItem(cand.title))
            self.table.setItem(row, 1, QTableWidgetItem(cand.artist))
            self.table.setItem(row, 2, QTableWidgetItem(cand.album))
            self.table.setItem(row, 3, QTableWidgetItem(cand.date))
            self.table.setItem(row, 4, QTableWidgetItem(cand.genre))

    def _on_accept(self) -> None:
        selected_items = self.table.selectedItems()
        if selected_items:
            row = selected_items[0].row()
            self.selected_candidate = self.candidates[row]
        self.accept()

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

        # Ancora rigorosa das instâncias ao escopo da QFrame (self)
        left_col = QWidget(self)
        left_layout = QVBoxLayout(left_col)
        
        self.thumb_lbl = QLabel("Sem Pré-visualização", left_col)
        self.thumb_lbl.setFixedSize(320, 180)
        self.thumb_lbl.setObjectName("ThumbLabel")
        self.thumb_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.thumb_lbl.setScaledContents(True)
        
        self.stats_lbl = QLabel("Pronto para Analisar", left_col)
        self.stats_lbl.setObjectName("StatsLabel")
        self.stats_lbl.setWordWrap(True)
        
        left_layout.addWidget(self.thumb_lbl)
        left_layout.addWidget(self.stats_lbl)
        left_layout.addStretch()

        right_container = QWidget(self)
        right_layout = QVBoxLayout(right_container)
        
        type_grp = QGroupBox("Selecionar Tipo de Multimédia", right_container)
        type_layout = QHBoxLayout(type_grp)
        self.rb_video = QRadioButton("Vídeo + Áudio", type_grp)
        self.rb_audio = QRadioButton("Apenas Áudio", type_grp)
        self.rb_audio.setChecked(True)
        
        self.btn_grp_type = QButtonGroup(self)
        self.btn_grp_type.addButton(self.rb_video)
        self.btn_grp_type.addButton(self.rb_audio)
        
        self.rb_video.toggled.connect(self._update_ui_mode)
        self.rb_audio.toggled.connect(self._update_ui_mode)
        
        type_layout.addWidget(self.rb_audio)
        type_layout.addWidget(self.rb_video)
        type_layout.addStretch()
        
        self.tabs = QTabWidget(right_container)
        
        # --- TAB FORMATO ---
        tab_format = QWidget(self.tabs)
        fmt_layout = QVBoxLayout(tab_format)
        fmt_grid = QGridLayout()
        
        self.cb_container = QComboBox(tab_format)
        self.cb_container.currentTextChanged.connect(self._on_container_changed)
        
        self.cb_abitrate = QComboBox(tab_format)
        for k in ["320", "256", "192", "128", "96"]:
            self.cb_abitrate.addItem(f"{k} kbps", k)
            
        self.cb_asr = QComboBox(tab_format)
        self.cb_asr.addItem("Auto", "auto")
        self.cb_asr.addItem("44.1 kHz", "44100")
        self.cb_asr.addItem("48.0 kHz", "48000")
        self.cb_asr.addItem("88.2 kHz", "88200")
        self.cb_asr.addItem("96.0 kHz", "96000")
        self.cb_asr.addItem("192.0 kHz", "192000")
        
        self.cb_bitdepth = QComboBox(tab_format)
        self.cb_bitdepth.addItems(["Auto", "16-bit", "24-bit", "32-bit"])
        
        self.cb_quality = QComboBox(tab_format)
        self.cb_quality.addItems([
            "Melhor Disponível", "8K (4320p)", "4K (2160p)", "1440p (2K)", 
            "1080p60", "1080p", "720p60", "720p", "480p", "360p"
        ])
        
        self.cb_vcodec = QComboBox(tab_format)
        self.cb_vcodec.addItems(["Melhor", "H264", "VP9", "AV1"])
        
        self.cb_acodec = QComboBox(tab_format)
        self.cb_acodec.addItems(["Melhor", "AAC", "MP3", "Opus"])

        self.lbl_container = QLabel("Formato:", tab_format)
        self.lbl_abitrate = QLabel("Bitrate:", tab_format)
        self.lbl_asr = QLabel("Taxa de Amostragem:", tab_format)
        self.lbl_bitdepth = QLabel("Profundidade de Bits:", tab_format)
        self.lbl_quality = QLabel("Resolução:", tab_format)
        self.lbl_vcodec = QLabel("Codec de Vídeo:", tab_format)
        self.lbl_acodec = QLabel("Codec de Áudio:", tab_format)
        
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
        fmt_grid.addWidget(self.cb_asr, 3, 3)
        fmt_grid.addWidget(self.lbl_bitdepth, 4, 0)
        fmt_grid.addWidget(self.cb_bitdepth, 4, 1)
        
        fmt_layout.addLayout(fmt_grid)
        fmt_layout.addStretch()
        
        # --- TAB METADATA ---
        tab_meta = QWidget(self.tabs)
        meta_scroll = QScrollArea(tab_meta)
        meta_scroll.setWidgetResizable(True)
        meta_scroll.setFrameShape(QFrame.Shape.NoFrame)
        meta_content = QWidget(meta_scroll)
        meta_form = QFormLayout(meta_content)
        
        self.btn_fetch_mb = QPushButton("Preenchimento Automático (MusicBrainz)", meta_content)
        self.btn_fetch_mb.setObjectName("PrimaryAction")
        self.btn_fetch_mb.clicked.connect(self._trigger_musicbrainz_fetch)
        
        self.in_filename = QLineEdit(meta_content)
        self.in_title = QLineEdit(meta_content)
        self.in_artist = QLineEdit(meta_content)
        self.in_album = QLineEdit(meta_content)
        self.in_genre = QLineEdit(meta_content)
        self.in_date = QLineEdit(meta_content)
        self.in_desc = QPlainTextEdit(meta_content)
        self.in_desc.setFixedHeight(60)
        
        meta_form.addRow("", self.btn_fetch_mb)
        meta_form.addRow("Nome do Ficheiro:", self.in_filename)
        meta_form.addRow("Título:", self.in_title)
        meta_form.addRow("Artista:", self.in_artist)
        meta_form.addRow("Álbum:", self.in_album)
        meta_form.addRow("Género:", self.in_genre)
        meta_form.addRow("Data (AAAA):", self.in_date)
        meta_form.addRow("Descrição:", self.in_desc)
        
        meta_scroll.setWidget(meta_content)
        meta_layout = QVBoxLayout(tab_meta)
        meta_layout.addWidget(meta_scroll)

        # --- TAB AVANÇADA ---
        tab_adv = QWidget(self.tabs)
        adv_layout = QVBoxLayout(tab_adv)
        
        chk_group = QGroupBox("Opções Standard", tab_adv)
        chk_layout = QVBoxLayout(chk_group)
        self.chk_meta = QCheckBox("Embutir Metadados", chk_group)
        self.chk_thumb = QCheckBox("Embutir Miniatura", chk_group)
        self.chk_subs = QCheckBox("Transferir Legendas", chk_group)
        self.chk_norm = QCheckBox("Normalização de Áudio", chk_group)
        self.chk_cookies = QCheckBox("Utilizar Cookies do Navegador", chk_group)
        
        self.chk_meta.setChecked(True)
        self.chk_thumb.setChecked(True)
        
        chk_layout.addWidget(self.chk_meta)
        chk_layout.addWidget(self.chk_thumb)
        chk_layout.addWidget(self.chk_subs)
        chk_layout.addWidget(self.chk_norm)
        chk_layout.addWidget(self.chk_cookies)
        
        dev_group = QGroupBox("Desenvolvimento e Personalização", tab_adv)
        dev_form = QFormLayout(dev_group)
        
        tmpl_layout = QHBoxLayout()
        tmpl_layout.setContentsMargins(0, 0, 0, 0)
        # Template atualizado para Música - Artista conforme solicitado
        self.in_output_tmpl = QLineEdit("%(title)s - %(artist)s.%(ext)s", dev_group)
        
        btn_tmpl_help = QPushButton("?", dev_group)
        btn_tmpl_help.setFixedWidth(30)
        btn_tmpl_help.clicked.connect(self._show_template_tutorial)
        
        tmpl_layout.addWidget(self.in_output_tmpl)
        tmpl_layout.addWidget(btn_tmpl_help)
        
        self.in_ffmpeg_path = QLineEdit(dev_group)
        btn_browse_ffmpeg = QPushButton("Procurar", dev_group)
        btn_browse_ffmpeg.clicked.connect(self._browse_ffmpeg)
        
        ffmpeg_layout = QHBoxLayout()
        ffmpeg_layout.setContentsMargins(0, 0, 0, 0)
        ffmpeg_layout.addWidget(self.in_ffmpeg_path)
        ffmpeg_layout.addWidget(btn_browse_ffmpeg)
        
        self.in_custom_flags = QLineEdit(dev_group)
        
        dev_form.addRow("Template de Saída:", tmpl_layout)
        dev_form.addRow("Caminho FFmpeg:", ffmpeg_layout)
        dev_form.addRow("Flags Personalizadas:", self.in_custom_flags)
        
        adv_layout.addWidget(chk_group)
        adv_layout.addWidget(dev_group)
        adv_layout.addStretch()

        # Acoplar Tabs
        self.tabs.addTab(tab_format, "Formato e Qualidade")
        self.tabs.addTab(tab_meta, "Metadados e Ficheiro")
        self.tabs.addTab(tab_adv, "Configuração Avançada")

        right_layout.addWidget(type_grp)
        right_layout.addWidget(self.tabs)
        
        main_layout.addWidget(left_col, 1)
        main_layout.addWidget(right_container, 2)
        
        self._update_ui_mode()

    def _trigger_musicbrainz_fetch(self) -> None:
        title = self.in_title.text().strip()
        artist = self.in_artist.text().strip()
        album = self.in_album.text().strip()
        date = self.in_date.text().strip()
        
        if not title and not artist and not album:
            QMessageBox.information(self, "Aviso", "Preencha ao menos o 'Título', 'Artista' ou 'Álbum' como base de pesquisa.")
            return

        self.btn_fetch_mb.setEnabled(False)
        self.btn_fetch_mb.setText("A procurar...")
        
        worker = MusicBrainzWorker(title, artist, album, date, APP_NAME, VERSION)
        worker.signals.results_ready.connect(self._on_musicbrainz_results)
        worker.signals.error.connect(self._on_musicbrainz_error)
        
        QThreadPool.globalInstance().start(worker)

    @pyqtSlot(list)
    def _on_musicbrainz_results(self, candidates: List[MetadataCandidate]) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.btn_fetch_mb): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        
        if not candidates:
            QMessageBox.information(self, "MusicBrainz", "Nenhum resultado encontrado.")
            return
            
        dialog = MetadataSelectionDialog(candidates, self)
        if dialog.exec() == QDialog.DialogCode.Accepted and dialog.selected_candidate:
            cand = dialog.selected_candidate
            if cand.title and not sip.isdeleted(self.in_title): self.in_title.setText(cand.title)
            if cand.artist and not sip.isdeleted(self.in_artist): self.in_artist.setText(cand.artist)
            if cand.album and not sip.isdeleted(self.in_album): self.in_album.setText(cand.album)
            if cand.date and not sip.isdeleted(self.in_date): self.in_date.setText(cand.date)
            if cand.genre and not sip.isdeleted(self.in_genre): self.in_genre.setText(cand.genre)
            
            # Transferência da arte original com lógica de resiliência SSL
            if cand.release_id:
                self.btn_fetch_mb.setText("A transferir arte HD...")
                self.btn_fetch_mb.setEnabled(False)
                
                ca_worker = CoverArtWorker(cand.release_id)
                ca_worker.signals.result_ready.connect(self._on_cover_art_ready)
                ca_worker.signals.error.connect(self._on_cover_art_error)
                QThreadPool.globalInstance().start(ca_worker)

    @pyqtSlot(QImage)
    def _on_cover_art_ready(self, image: QImage) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.btn_fetch_mb): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        
        pixmap = QPixmap.fromImage(image)
        self.set_thumbnail(pixmap)
        logging.info("Arte de alta resolução aplicada via Cover Art Archive.")

    @pyqtSlot(str)
    def _on_cover_art_error(self, err_msg: str) -> None:
        """
        Garante que, em caso de erro na obtenção da arte HD, a miniatura 
        original do vídeo seja preservada sem alterações.
        """
        if sip.isdeleted(self) or sip.isdeleted(self.btn_fetch_mb): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        logging.warning(f"Resolução da Arte ignorada (Original preservada): {err_msg}")

    @pyqtSlot(str)
    def _on_musicbrainz_error(self, err_msg: str) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.btn_fetch_mb): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        QMessageBox.warning(self, "Erro MusicBrainz", f"Falha na API:\n{err_msg}")

    def _show_template_tutorial(self) -> None:
        tutorial_html = (
            "<h3>Guia de Nomenclatura Dinâmica (Output Template)</h3>"
            "<p>O template de saída orquestra a montagem estruturada dos diretórios e ficheiros.</p>"
            "<br>"
            "<b>Variáveis de Contexto Disponíveis:</b>"
            "<ul>"
            "<li><code>%(title)s</code>: Título literal da faixa</li>"
            "<li><code>%(artist)s</code>: Nome do Artista</li>"
            "<li><code>%(album)s</code>: Nome do Álbum</li>"
            "<li><code>%(genre)s</code>: Género Musical</li>"
            "<li><code>%(release_year)s</code>: Ano de Lançamento (YYYY)</li>"
            "<li><code>%(ext)s</code>: Extensão do ficheiro (ex: mp3, flac)</li>"
            "<li><code>%(uploader)s</code>: Entidade ou canal remetente</li>"
            "<li><code>%(upload_date)s</code>: Data de transmissão original (YYYYMMDD)</li>"
            "<li><code>%(playlist)s</code>: Nome da Playlist/Álbum de origem</li>"
            "<li><code>%(playlist_index)s</code>: Índice sequencial (Track Number)</li>"
            "</ul>"
        )
        msg_box = QMessageBox(self)
        msg_box.setWindowTitle("Tutorial: yt-dlp Output Templates")
        msg_box.setText(tutorial_html)
        msg_box.exec()

    def _browse_ffmpeg(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Selecionar Executável FFmpeg", "", "Executáveis (*.exe);;Todos os Ficheiros (*)")
        if path:
            self.in_ffmpeg_path.setText(path)

    def set_metadata(self, meta: proc.MediaMetadata) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.in_filename): return
        self._current_meta = meta
        
        # Sanitização agressiva para evitar Errno 22 (caracteres ilegais no Windows)
        sanitized_title = re.sub(r'[\x00-\x1f\x7f]', '', meta.title)
        sanitized_title = re.sub(r'[<>:"/\\|?*]', '', sanitized_title)
        sanitized_title = re.sub(r'\s+', ' ', sanitized_title).strip()
        
        self.in_filename.setText(sanitized_title)
        
        if not sip.isdeleted(self.in_title): self.in_title.setText(meta.title)
        if not sip.isdeleted(self.in_artist): self.in_artist.setText(meta.artist)
        if not sip.isdeleted(self.in_album): self.in_album.setText(meta.album)
        if not sip.isdeleted(self.in_date): self.in_date.setText(meta.upload_date[:4] if meta.upload_date else "")
        if not sip.isdeleted(self.in_desc): self.in_desc.setPlainText(meta.description)
        if not sip.isdeleted(self.in_genre): self.in_genre.clear()
        
        self._recalc_estimate()

    def set_thumbnail(self, pixmap: QPixmap) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.thumb_lbl): return
        self.thumb_lbl.setPixmap(pixmap)

    def _update_ui_mode(self) -> None:
        is_video = self.rb_video.isChecked()
        
        self.cb_container.blockSignals(True)
        self.cb_container.clear()
        if is_video:
            self.cb_container.addItems(["mp4", "mkv", "webm"])
        else:
            self.cb_container.addItems(["flac", "mp3", "wav", "m4a", "opus"])
        self.cb_container.blockSignals(False)
        self.cb_container.setCurrentIndex(0)
        
        video_widgets = [self.lbl_quality, self.cb_quality, self.lbl_vcodec, self.cb_vcodec, self.lbl_acodec, self.cb_acodec]
        for w in video_widgets:
            w.setVisible(is_video)
            
        audio_widgets = [self.lbl_abitrate, self.cb_abitrate, self.lbl_asr, self.cb_asr]
        for w in audio_widgets:
            w.setVisible(not is_video)
            
        self.chk_norm.setVisible(not is_video)
        self._on_container_changed(self.cb_container.currentText())

    def _on_container_changed(self, fmt: str) -> None:
        is_lossless = fmt in ['flac', 'wav']
        
        self.cb_abitrate.setEnabled(not is_lossless)
        self.lbl_bitdepth.setVisible(is_lossless)
        self.cb_bitdepth.setVisible(is_lossless)
        
        if is_lossless:
            self.lbl_abitrate.setText("Bitrate (Lossless):")
        else:
            self.lbl_abitrate.setText("Bitrate:")
            
        self._recalc_estimate()

    def _recalc_estimate(self) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.stats_lbl) or not self._current_meta: return
        base_info = (
            f"<b>Duração:</b> {self._current_meta.display_duration}<br>"
            f"<b>Origem:</b> {self._current_meta.width}x{self._current_meta.height} @ {self._current_meta.fps}fps<br>"
            f"<b>Canal:</b> {self._current_meta.channel}"
        )
        self.stats_lbl.setText(base_info)

    def get_config_delta(self) -> Dict[str, Any]:
        """
        Extrai as configurações atuais da interface de forma thread-safe.
        Verifica se os objetos subjacentes em C++ ainda existem para evitar RuntimeErrors.
        """
        if sip.isdeleted(self) or sip.isdeleted(self.tabs):
            return {}

        asr_data = self.cb_asr.currentData() if not sip.isdeleted(self.cb_asr) else "auto"
        audio_sample_rate = 0 if asr_data == "auto" else int(asr_data)
        
        bitdepth_text = self.cb_bitdepth.currentText() if not sip.isdeleted(self.cb_bitdepth) else "Auto"
        audio_bit_depth = bitdepth_text.split('-')[0] if "bit" in bitdepth_text else "auto"

        return {
            'media_type': proc.MediaType.VIDEO if self.rb_video.isChecked() else proc.MediaType.AUDIO,
            'format_container': self.cb_container.currentText() if not sip.isdeleted(self.cb_container) else "mp3",
            'video_codec': self.cb_vcodec.currentText().lower() if not sip.isdeleted(self.cb_vcodec) else "best",
            'audio_codec': self.cb_acodec.currentText().lower() if not sip.isdeleted(self.cb_acodec) else "best",
            'quality_preset': self.cb_quality.currentText() if not sip.isdeleted(self.cb_quality) else "Best",
            'audio_bitrate': self.cb_abitrate.currentData() if not sip.isdeleted(self.cb_abitrate) and self.cb_abitrate.isEnabled() else "0",
            'audio_sample_rate': audio_sample_rate,
            'audio_bit_depth': audio_bit_depth, 
            'custom_filename': self.in_filename.text().strip() if not sip.isdeleted(self.in_filename) else "output",
            
            'output_template': self.in_output_tmpl.text().strip() if not sip.isdeleted(self.in_output_tmpl) else "",
            'ffmpeg_path': self.in_ffmpeg_path.text().strip() if not sip.isdeleted(self.in_ffmpeg_path) else "",
            'custom_flags': self.in_custom_flags.text().strip() if not sip.isdeleted(self.in_custom_flags) else "",
            
            'meta_title': self.in_title.text() if not sip.isdeleted(self.in_title) else "",
            'meta_artist': self.in_artist.text() if not sip.isdeleted(self.in_artist) else "",
            'meta_album': self.in_album.text() if not sip.isdeleted(self.in_album) else "",
            'meta_genre': self.in_genre.text() if not sip.isdeleted(self.in_genre) else "",
            'meta_date': self.in_date.text() if not sip.isdeleted(self.in_date) else "",
            'meta_desc': self.in_desc.toPlainText() if not sip.isdeleted(self.in_desc) else "",
            'embed_meta': self.chk_meta.isChecked() if not sip.isdeleted(self.chk_meta) else True,
            'embed_thumb': self.chk_thumb.isChecked() if not sip.isdeleted(self.chk_thumb) else True,
            'embed_subs': self.chk_subs.isChecked() if not sip.isdeleted(self.chk_subs) else False,
            'norm_audio': self.chk_norm.isChecked() if not sip.isdeleted(self.chk_norm) else False,
            'use_cookies': self.chk_cookies.isChecked() if not sip.isdeleted(self.chk_cookies) else False,
        }

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {VERSION}")
        self.resize(1200, 900)
        self.setMinimumSize(1000, 700)
        
        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(MAX_CONCURRENT_DOWNLOADS)
        self.active_runnables: Dict[str, proc.DownloadWorker] = {}
        self._current_meta: Optional[proc.MediaMetadata] = None
        
        self.qt_log_handler = QtLogHandler()
        logging.getLogger().addHandler(self.qt_log_handler)
        
        self.init_ui()
        self._init_menu_bar()
        self._apply_theme(is_dark=True) 

    def _init_menu_bar(self) -> None:
        menu_bar = self.menuBar()
        if menu_bar is None: return
        
        menu_file = menu_bar.addMenu("Ficheiro")
        action_exit = QAction("Sair", self)
        action_exit.triggered.connect(self.close)
        menu_file.addAction(action_exit)

        menu_view = menu_bar.addMenu("Ver")
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
        central = QWidget(self)
        self.setCentralWidget(central)
        
        main_splitter = QSplitter(Qt.Orientation.Vertical, central)
        top_container = QWidget(main_splitter)
        main_layout = QVBoxLayout(top_container)
        main_layout.setContentsMargins(20, 20, 20, 0)
        
        top_bar = QHBoxLayout()
        header = QLabel(APP_NAME.upper(), top_container)
        header.setObjectName("MainHeader")
        
        self.chk_dev_mode = QCheckBox("Modo de Desenvolvedor", top_container)
        self.chk_dev_mode.toggled.connect(self.toggle_dev_mode)
        
        top_bar.addWidget(header)
        top_bar.addStretch()
        top_bar.addWidget(self.chk_dev_mode)
        main_layout.addLayout(top_bar)

        input_frame = QFrame(top_container)
        input_frame.setObjectName("Panel")
        input_layout = QHBoxLayout(input_frame)
        self.url_input = QLineEdit(input_frame)
        self.url_input.setPlaceholderText("Colar URL...")
        self.btn_analyze = QPushButton("Analisar Multimédia", input_frame)
        self.btn_analyze.setObjectName("PrimaryAction")
        self.btn_analyze.setFixedHeight(40)
        self.btn_analyze.clicked.connect(self.start_analysis)
        input_layout.addWidget(self.url_input, 1)
        input_layout.addWidget(self.btn_analyze)
        main_layout.addWidget(input_frame)

        # Injeção explícita de parentalidade na criação do Panel
        self.inspector = InspectorPanel(top_container)
        self.inspector.setVisible(False)
        main_layout.addWidget(self.inspector)

        self.action_bar = QFrame(top_container)
        self.action_bar.setVisible(False)
        action_layout = QHBoxLayout(self.action_bar)
        self.path_input = QLineEdit(str(DEFAULT_DOWNLOAD_DIR), self.action_bar)
        self.path_input.setReadOnly(True)
        
        btn_path = QPushButton("Alterar Pasta", self.action_bar)
        btn_path.clicked.connect(self.browse_folder)
        
        self.btn_queue = QPushButton("Adicionar à Fila", self.action_bar)
        self.btn_queue.setObjectName("PrimaryAction")
        self.btn_queue.clicked.connect(self.queue_download)
        
        action_layout.addWidget(QLabel("Pasta de Saída:", self.action_bar))
        action_layout.addWidget(self.path_input)
        action_layout.addWidget(btn_path)
        action_layout.addStretch()
        action_layout.addWidget(self.btn_queue)
        main_layout.addWidget(self.action_bar)

        self.table = QTableWidget(top_container)
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels(["Ficheiro / Título", "Formato", "Estado", "Progresso", "Ações"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        header_view = self.table.horizontalHeader()
        if header_view is not None:
            header_view.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        
        self.table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_context_menu)
        
        main_layout.addWidget(self.table)
        
        self.log_viewer = LogViewerWidget(main_splitter)
        self.log_viewer.setVisible(False)
        self.qt_log_handler.log_record.connect(self.log_viewer.append_log)
        
        main_splitter.addWidget(top_container)
        main_splitter.addWidget(self.log_viewer)
        main_splitter.setSizes([800, 200])
        main_splitter.setCollapsible(0, False)
        
        final_layout = QVBoxLayout(central)
        final_layout.setContentsMargins(0,0,0,0)
        final_layout.addWidget(main_splitter)

    def _show_context_menu(self, pos: Any) -> None:
        item = self.table.itemAt(pos)
        menu = QMenu(self)
        
        if item is not None:
            row = item.row()
            cell_item = self.table.item(row, 0)
            if cell_item is not None:
                job_id = cell_item.data(Qt.ItemDataRole.UserRole)
                
                action_remove = QAction("Remover da Fila", self)
                action_remove.triggered.connect(lambda _, jid=job_id: self._remove_from_queue(jid))
                menu.addAction(action_remove)
                
                if job_id in self.active_runnables:
                    action_cancel = QAction("Cancelar Transferência", self)
                    action_cancel.triggered.connect(lambda _, jid=job_id: self.cancel_job(jid))
                    menu.addAction(action_cancel)
                    
                menu.addSeparator()
            
        action_open = QAction("Abrir Pasta de Saída", self)
        action_open.triggered.connect(self.open_output_folder)
        menu.addAction(action_open)
        
        action_clear = QAction("Limpar Concluídos", self)
        action_clear.triggered.connect(self._clear_finished_jobs)
        menu.addAction(action_clear)
        
        viewport = self.table.viewport()
        if viewport is not None:
            menu.exec(viewport.mapToGlobal(pos))

    def _remove_from_queue(self, job_id: str) -> None:
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            del self.active_runnables[job_id]
            
        row = self.get_row_by_id(job_id)
        if row >= 0:
            self.table.removeRow(row)

    def _clear_finished_jobs(self) -> None:
        for row in range(self.table.rowCount() - 1, -1, -1):
            status_item = self.table.item(row, 2)
            if status_item is not None and status_item.text() in ["✔ Concluído", "✘ Erro", "Cancelado"]:
                self.table.removeRow(row)

    def toggle_dev_mode(self, checked: bool) -> None:
        logging.getLogger().setLevel(logging.DEBUG if checked else logging.INFO)
        self.log_viewer.setVisible(checked)

    def start_analysis(self) -> None:
        url = self.url_input.text().strip()
        if not url: return
        
        if not proc.YtDlpService.validate_url(url):
            QMessageBox.warning(self, "URL Inválido", "O padrão de URL fornecido não é suportado.")
            return
        
        self.btn_analyze.setEnabled(False)
        self.btn_analyze.setText("A processar...")
        worker = proc.AnalysisWorker(url)
        worker.signals.result.connect(self.on_analysis_success)
        worker.signals.thumbnail_data.connect(self.on_thumbnail_ready)
        worker.signals.error.connect(self.on_analysis_error)
        worker.signals.finished.connect(lambda: self.btn_analyze.setEnabled(True) if not sip.isdeleted(self.btn_analyze) else None)
        worker.signals.finished.connect(lambda: self.btn_analyze.setText("Analisar Multimédia") if not sip.isdeleted(self.btn_analyze) else None)
        self.thread_pool.start(worker)

    @pyqtSlot(object)
    def on_analysis_success(self, meta: proc.MediaMetadata) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.inspector): return
        self._current_meta = meta
        self.inspector.set_metadata(meta)
        if not sip.isdeleted(self.inspector):
            self.inspector.setVisible(True)
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(True)

    @pyqtSlot(bytes)
    def on_thumbnail_ready(self, data: bytes) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.inspector): return
        pixmap = QPixmap()
        pixmap.loadFromData(data)
        self.inspector.set_thumbnail(pixmap)

    @pyqtSlot(str)
    def on_analysis_error(self, err_msg: str) -> None:
        if sip.isdeleted(self): return
        QMessageBox.critical(self, "Falha na Análise", err_msg)
        if not sip.isdeleted(self.inspector):
            self.inspector.setVisible(False)
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(False)

    def browse_folder(self) -> None:
        d = QFileDialog.getExistingDirectory(self, "Localização de Guarda", self.path_input.text())
        if d: self.path_input.setText(d)

    def open_output_folder(self) -> None:
        path_str = self.path_input.text()
        if path_str:
            path_obj = Path(path_str)
            if path_obj.exists() and path_obj.is_dir():
                QDesktopServices.openUrl(QUrl.fromLocalFile(str(path_obj)))
            else:
                QMessageBox.warning(self, "Aviso", "O diretório especificado não existe ou está inacessível.")

    def queue_download(self) -> None:
        if not self._current_meta: return
        
        # Extração de dados com validação de ponteiros sip
        data = self.inspector.get_config_delta()
        if not data:
            return
        
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
        
        object.__setattr__(config, "audio_bit_depth", data.get('audio_bit_depth', 'auto'))
        object.__setattr__(config, "output_template", data.get('output_template', ''))
        object.__setattr__(config, "ffmpeg_path", data.get('ffmpeg_path', ''))
        object.__setattr__(config, "custom_flags", data.get('custom_flags', ''))

        self._spawn_download(config)
        
        if not sip.isdeleted(self.inspector):
            self.inspector.setVisible(False)
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(False)
        self.url_input.clear()

    def _spawn_download(self, config: proc.DownloadJobConfig) -> None:
        runnable = proc.DownloadWorker(config)
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
        
        btn_cancel = QPushButton("Parar")
        btn_cancel.setObjectName("Destructive")
        btn_cancel.clicked.connect(lambda _, jid=config.job_id: self.cancel_job(jid))
        
        btn_layout.addWidget(btn_cancel)
        btn_container.setLayout(btn_layout)
        
        self.table.setItem(row, 0, title_item)
        self.table.setItem(row, 1, QTableWidgetItem(fmt_str))
        self.table.setItem(row, 2, QTableWidgetItem("Na Fila"))
        self.table.setCellWidget(row, 3, pbar)
        self.table.setCellWidget(row, 4, btn_container)

    def get_row_by_id(self, job_id: str) -> int:
        for r in range(self.table.rowCount()):
            item = self.table.item(r, 0)
            if item is not None and item.data(Qt.ItemDataRole.UserRole) == job_id: return r
        return -1

    @pyqtSlot(str, float, str)
    def update_progress(self, job_id: str, pct: float, speed: str) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.table): return
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
        if sip.isdeleted(self) or sip.isdeleted(self.table): return
        row = self.get_row_by_id(job_id)
        if row >= 0:
            item = self.table.item(row, 2)
            if item is not None:
                item.setText(msg)

    def on_job_finished(self, job_id: str) -> None:
        if sip.isdeleted(self): return
        self._cleanup_job(job_id, "✔ Concluído", QColor("#4caf50"))

    def on_job_error(self, job_id: str, err: str) -> None:
        if sip.isdeleted(self): return
        self._cleanup_job(job_id, "✘ Erro", QColor("#d32f2f"))

    def cancel_job(self, job_id: str) -> None:
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            self._cleanup_job(job_id, "Cancelado", QColor("#ff9800"))

    def _cleanup_job(self, job_id: str, status_text: str, color: QColor) -> None:
        row = self.get_row_by_id(job_id)
        if row >= 0:
            item = self.table.item(row, 2)
            if item is not None:
                item.setText(status_text)
                item.setForeground(color)
            
            if "Concluído" in status_text: 
                widget = self.table.cellWidget(row, 3)
                if isinstance(widget, QProgressBar):
                    widget.setValue(100)
                
                btn_open = QPushButton("Abrir Pasta")
                btn_open.clicked.connect(self.open_output_folder)
                
                layout = QHBoxLayout()
                layout.setContentsMargins(0, 0, 0, 0)
                layout.addWidget(btn_open)
                container = QWidget()
                container.setLayout(layout)
                self.table.setCellWidget(row, 4, container)
                
            elif "Cancelado" in status_text:
                widget = self.table.cellWidget(row, 3)
                if isinstance(widget, QProgressBar):
                    widget.setValue(0)
                self.table.setCellWidget(row, 4, QWidget())
                
            elif "Erro" in status_text:
                self.table.setCellWidget(row, 4, QWidget())
                
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
    logging.critical("Exceção não tratada:", exc_info=(exc_type, exc_value, exc_traceback))

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