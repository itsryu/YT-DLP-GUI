import os
import sys
import logging
import threading
import uuid
import re
import types
import json
import urllib.parse
import urllib.request
import urllib.error
import time
import ssl
import tempfile
from dataclasses import dataclass
from typing import Callable, Final, Dict, Any, Optional, List, TypeVar
from pathlib import Path
from functools import wraps

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QMessageBox, QFrame, QScrollArea, QGroupBox, QFormLayout, 
    QCheckBox, QPlainTextEdit, QSplitter, QTabWidget, QRadioButton, 
    QButtonGroup, QAbstractItemView, QMenu, QDialog, QDialogButtonBox
)
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThreadPool, pyqtSlot, QUrl, QRunnable, QTimer
from PyQt6.QtGui import QColor, QPixmap, QFont, QTextCursor, QTextCharFormat, QDesktopServices, QPalette, QAction, QCloseEvent, QImage
from PyQt6 import sip

import processamento as proc

# =====================================================================
# INJEÇÃO DE AMBIENTE SANDBOXED
# O yt-dlp utiliza o Deno para executar algoritmos JS de resolução de URL.
# Para evitar que o Deno tente acessar o sistema de arquivos ou a rede diretamente,
# injetamos variáveis de ambiente que apontam para locais controlados e seguros.
# =====================================================================
PROJECT_DIR = str(Path(__file__).parent.absolute())
if PROJECT_DIR not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"{PROJECT_DIR};{os.environ.get('PATH', '')}"
# =====================================================================

T = TypeVar('T')

APP_NAME: Final[str] = "SoundStream Pro"
VERSION: Final[str] = "7.3.0"
DEFAULT_DOWNLOAD_DIR: Final[Path] = Path.home() / "Downloads"
MAX_CONCURRENT_DOWNLOADS: Final[int] = 3

class DialogThumbnailSignals(QObject):
    ready = pyqtSignal(int, QImage)

class DialogThumbnailWorker(QRunnable):
    def __init__(self, row_index: int, release_id: str, release_group_id: str) -> None:
        super().__init__()
        self.row_index = row_index
        self.release_id = release_id
        self.release_group_id = release_group_id
        self.signals = DialogThumbnailSignals()
        self.ctx = ssl.create_default_context()
        self.headers = {"User-Agent": f"{APP_NAME}/{VERSION}", "Connection": "close"}

    @pyqtSlot()
    def run(self) -> None:
        endpoints = []
        if self.release_id: endpoints.append(f"https://coverartarchive.org/release/{self.release_id}/front-250")
        if self.release_group_id: endpoints.append(f"https://coverartarchive.org/release-group/{self.release_group_id}/front-250")

        for url in endpoints:
            try:
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req, timeout=8, context=self.ctx) as response:
                    buffer = response.read()
                    image = QImage()
                    if image.loadFromData(buffer):
                        self.signals.ready.emit(self.row_index, image)
                        return
            except urllib.error.HTTPError as e:
                if e.code in (404, 400, 403, 503): continue 
            except Exception as e:
                logging.debug(f"[Grid] Falha tolerável na resolução de miniatura rápida: {e}")
                continue

class APIRateLimiter:
    def __init__(self, rate: float, capacity: int) -> None:
        self.rate: float = rate
        self.capacity: int = capacity
        self.tokens: float = float(capacity)
        self.last_update: float = time.monotonic()
        self.condition: threading.Condition = threading.Condition()

    def wait(self) -> None:
        with self.condition:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                
                sleep_time = (1.0 - self.tokens) / self.rate
                self.condition.wait(timeout=sleep_time)

mb_rate_limiter = APIRateLimiter(rate=1.0, capacity=1)

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
    release_id: str
    release_group_id: str

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
        return re.sub(r'([+\-!(){}\[\]^"~*?:\\])', r'\\\1', text)

    @pyqtSlot()
    def run(self) -> None:
        try:
            query_parts = []
            if self.title_query: query_parts.append(f'recording:"{self._escape_lucene(self.title_query)}"')
            if self.artist_query: query_parts.append(f'artist:"{self._escape_lucene(self.artist_query)}"')
            if self.album_query: query_parts.append(f'release:"{self._escape_lucene(self.album_query)}"')
            if self.date_query: query_parts.append(f'date:"{self._escape_lucene(self.date_query)}"')
            
            if not query_parts:
                self.signals.results_ready.emit([])
                return

            encoded_query = urllib.parse.quote(" AND ".join(query_parts))
            url = f"https://musicbrainz.org/ws/2/recording/?query={encoded_query}&fmt=json"
            
            headers = {
                "User-Agent": f"{self.app_name}/{self.version} ( dev@localhost )",
                "Accept": "application/json",
                "Connection": "keep-alive"
            }
            req = urllib.request.Request(url, headers=headers)
            ctx = ssl.create_default_context()
            
            mb_rate_limiter.wait()
            logging.debug(f"[Network] A consultar AST Lucene MusicBrainz: {url}")
            
            with urllib.request.urlopen(req, timeout=15, context=ctx) as response:
                data = json.loads(response.read().decode('utf-8'))
                
            candidates: List[MetadataCandidate] = []
            for rec in data.get("recordings", [])[:15]: 
                title = rec.get("title", "")
                artist = "".join([ac.get("name", "") + ac.get("joinphrase", "") for ac in rec.get("artist-credit", [])])
                
                releases = rec.get("releases", [])
                album = releases[0].get("title", "") if releases else ""
                date = releases[0].get("date", "")[:4] if releases and releases[0].get("date") else ""
                release_id = releases[0].get("id", "") if releases else ""
                release_group_id = releases[0].get("release-group", {}).get("id", "") if releases else ""
                
                tags = rec.get("tags", [])
                genre = tags[0].get("name", "").title() if tags else ""
                
                candidates.append(MetadataCandidate(title, artist, album, date, genre, release_id, release_group_id))
                
            self.signals.results_ready.emit(candidates)
            
        except urllib.error.URLError as e:
            logging.error(f"[Network] Falha na resolução de soquetes: {e}")
            self.signals.error.emit(str(e))
        except json.JSONDecodeError as e:
            logging.error(f"[Parser] Árvore JSON corrompida: {e}")
            self.signals.error.emit("A resposta do servidor não é um JSON válido.")
        except Exception as e:
            logging.critical(f"[System] Exceção fatal na camada de busca: {e}", exc_info=True)
            self.signals.error.emit(str(e))

class CoverArtWorker(QRunnable):
    def __init__(self, release_id: str, release_group_id: str) -> None:
        super().__init__()
        self.release_id: str = release_id
        self.release_group_id: str = release_group_id
        self.signals: CoverArtSignals = CoverArtSignals()
        self.ctx: ssl.SSLContext = ssl.create_default_context()
        self.headers: Dict[str, str] = {
            "User-Agent": f"SoundStreamPro/7.3.0 ( dev@localhost )",
            "Accept": "application/json",
            "Connection": "keep-alive"
        }

    def with_exponential_backoff(max_retries: int = 3, base_delay: float = 1.0) -> Callable[[Callable[..., T]], Callable[..., T]]:
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                attempt = 0
                while attempt < max_retries:
                    try:
                        return func(*args, **kwargs)
                    except (urllib.error.URLError, ssl.SSLError, ConnectionError) as e:
                        error_msg = str(e)
                        if "UNEXPECTED_EOF" in error_msg or "EOF occurred" in error_msg or "Connection reset" in error_msg:
                            attempt += 1
                            if attempt >= max_retries:
                                raise
                            
                            delay = base_delay * (2 ** attempt)
                            logging.warning(f"[Network] Interrupção prematura de TCP/TLS. Retentativa {attempt}/{max_retries} pendente (Atraso: {delay}s).")
                            time.sleep(delay)
                        else:
                            raise
            return wrapper
        return decorator

    @with_exponential_backoff(max_retries=5, base_delay=1.0)
    def _fetch_json_manifest(self, endpoint_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        if not entity_id: return None
        
        url = f"https://coverartarchive.org/{endpoint_type}/{entity_id}"
        req = urllib.request.Request(url, headers=self.headers)
        
        mb_rate_limiter.wait()
        logging.debug(f"[Network] A analisar manifesto do {endpoint_type}: {entity_id}")
        
        try:
            with urllib.request.urlopen(req, timeout=15, context=self.ctx) as response:
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code in (404, 400):
                logging.warning(f"[Network] Manifesto indisponível (HTTP {e.code}) no nó {endpoint_type}.")
                return None
            raise
        except Exception as e:
            logging.error(f"[Network] Falha na negociação com {url}: {e}")
            raise

    def _extract_optimal_resolution(self, manifest: Dict[str, Any]) -> Optional[str]:
        images: List[Dict[str, Any]] = manifest.get("images", [])
        if not images:
            return None

        for img in images:
            is_front = img.get("front", False) or "Front" in img.get("types", [])
            if is_front:
                thumbnails: Dict[str, str] = img.get("thumbnails", {})
                if "1200" in thumbnails: return thumbnails["1200"]
                if "500" in thumbnails: return thumbnails["500"]
                if "250" in thumbnails: return thumbnails["250"]
                return img.get("image")
        
        return images[0].get("image")

    @pyqtSlot()
    def run(self) -> None:
        try:
            manifest = self._fetch_json_manifest("release", self.release_id)
            
            if not manifest and self.release_group_id:
                logging.info(f"[Parser] Executando Fallback Heurístico para Release Group: {self.release_group_id}")
                manifest = self._fetch_json_manifest("release-group", self.release_group_id)

            if not manifest:
                self.signals.error.emit("A entidade requisitada e o seu grupo não possuem metadados visuais arquivados.")
                return

            optimal_url = self._extract_optimal_resolution(manifest)
            if not optimal_url:
                self.signals.error.emit("Nenhum nó de imagem front/thumbnail classificado no manifesto.")
                return

            logging.info(f"[Process] Fluxo binário selecionado: {optimal_url}")
            req = urllib.request.Request(optimal_url, headers={"User-Agent": self.headers["User-Agent"]})
            
            with urllib.request.urlopen(req, timeout=30, context=self.ctx) as response:
                buffer = response.read()

            image = QImage()
            if image.loadFromData(buffer):
                self.signals.result_ready.emit(image)
            else:
                logging.error("[Parser] O buffer de memória contém um cabeçalho de imagem inválido ou corrompido.")
                self.signals.error.emit("Falha no decodificador de matriz de bits da imagem.")

        except Exception as e:
            logging.error(f"[System] Colapso na propagação da árvore de capa: {str(e)}")
            self.signals.error.emit(f"Falha na alocação da árvore de arte: {str(e)}")

class MetadataSelectionDialog(QDialog):
    def __init__(self, candidates: List[MetadataCandidate], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("MusicBrainz: Selecionar Metadados")
        self.setMinimumSize(850, 450)
        self.selected_candidate: Optional[MetadataCandidate] = None
        self.candidates = candidates
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        
        self.table = QTableWidget(self)
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["Capa", "Título", "Artista", "Álbum", "Ano", "Género"])
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.verticalHeader().setDefaultSectionSize(60)
        
        header = self.table.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
            self.table.setColumnWidth(0, 60)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
            header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        
        self._populate_table()
        layout.addWidget(self.table)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel, self)
        btn_box.accepted.connect(self._on_accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def _populate_table(self) -> None:
        self.table.setRowCount(len(self.candidates))
        for row, cand in enumerate(self.candidates):
            lbl_cover = QLabel("...", self.table)
            lbl_cover.setAlignment(Qt.AlignmentFlag.AlignCenter)
            lbl_cover.setStyleSheet("color: gray; font-size: 10px;")
            self.table.setCellWidget(row, 0, lbl_cover)
            
            self.table.setItem(row, 1, QTableWidgetItem(cand.title))
            self.table.setItem(row, 2, QTableWidgetItem(cand.artist))
            self.table.setItem(row, 3, QTableWidgetItem(cand.album))
            self.table.setItem(row, 4, QTableWidgetItem(cand.date))
            self.table.setItem(row, 5, QTableWidgetItem(cand.genre))

            if cand.release_id or cand.release_group_id:
                worker = DialogThumbnailWorker(row, cand.release_id, cand.release_group_id)
                worker.signals.ready.connect(self._apply_thumbnail)
                QThreadPool.globalInstance().start(worker)
                
    @pyqtSlot(int, QImage)
    def _apply_thumbnail(self, row: int, image: QImage) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.table): return
        
        pixmap = QPixmap.fromImage(image).scaled(
            50, 50, 
            Qt.AspectRatioMode.KeepAspectRatio, 
            Qt.TransformationMode.SmoothTransformation
        )
        
        lbl_cover = QLabel(self.table)
        lbl_cover.setAlignment(Qt.AlignmentFlag.AlignCenter)
        lbl_cover.setPixmap(pixmap)
        self.table.setCellWidget(row, 0, lbl_cover)

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
        self._current_meta: Optional[proc.NormalizedMediaEntity] = None
        self._init_ui()

    def _init_ui(self) -> None:
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(20)

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
        self.cb_vcodec.addItems(["Melhor", "H264", "VP9", "AV1", "DivX", "XviD"])
        
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
            
            if cand.release_id:
                self.btn_fetch_mb.setText("A transferir arte HD...")
                self.btn_fetch_mb.setEnabled(False)
                
                ca_worker = CoverArtWorker(cand.release_id, cand.release_group_id)
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
        
        import tempfile
        cache_dir = Path(tempfile.gettempdir())
        self._custom_cover_path = cache_dir / f"cover_sndstream_custom_{uuid.uuid4().hex[:8]}.jpg"

        safe_image = image.convertToFormat(QImage.Format.Format_RGB32)
        safe_image.save(str(self._custom_cover_path), "JPG", 95)
        
        logging.info(f"[I/O] Arte primária fixada e convertida (RGB32). Sobrescrita bloqueada: {self._custom_cover_path}")

    @pyqtSlot(str)
    def _on_cover_art_error(self, err_msg: str) -> None:
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

    def set_metadata(self, meta: proc.NormalizedMediaEntity) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.in_filename): return
        self._current_meta = meta
        
        sanitized_title = re.sub(r'[\x00-\x1f\x7f]', '', meta.title)
        sanitized_title = re.sub(r'[<>:"/\\|?*]', '', sanitized_title)
        sanitized_title = re.sub(r'\s+', ' ', sanitized_title).strip()
        
        self.in_filename.setText(sanitized_title)
        
        if not sip.isdeleted(self.in_title): self.in_title.setText(meta.title)
        if not sip.isdeleted(self.in_artist): self.in_artist.setText(meta.artist)
        if not sip.isdeleted(self.in_album): self.in_album.setText(meta.album)
        
        if not sip.isdeleted(self.in_date): 
            self.in_date.setText(meta.upload_date[:4] if meta.upload_date else "")
        if not sip.isdeleted(self.in_desc): 
            self.in_desc.setPlainText(meta.description if meta.description else "")
            
        if not sip.isdeleted(self.in_genre): self.in_genre.clear()
        
        is_audio_restricted = False
        orig_id = getattr(meta, 'original_id', '')
        if isinstance(orig_id, str) and ('ytmsearch' in orig_id or 'ytsearch' in orig_id):
            is_audio_restricted = True
            
        if getattr(meta, 'is_playlist', False) and getattr(meta, 'children', None):
            for c in meta.children:
                c_id = getattr(c, 'original_id', '')
                if isinstance(c_id, str) and ('ytmsearch' in c_id or 'ytsearch' in c_id):
                    is_audio_restricted = True
                    break

        if is_audio_restricted:
            self.rb_audio.setChecked(True)
            self.rb_video.setEnabled(False)
            self.rb_video.setToolTip("Vídeo bloqueado: A topologia de origem (YTM/Spotify) restringe a extração ao formato de áudio.")
        else:
            self.rb_video.setEnabled(True)
            self.rb_video.setToolTip("")
            
        self._recalc_estimate()

    def set_thumbnail(self, pixmap: QPixmap) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.thumb_lbl): return
        self.thumb_lbl.setPixmap(pixmap)

    def clear_thumbnail(self) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.thumb_lbl): return
        self.thumb_lbl.clear()
        self.thumb_lbl.setText("Sem Pré-visualização")

    def _update_ui_mode(self) -> None:
        is_video = self.rb_video.isChecked()
        
        self.cb_container.blockSignals(True)
        self.cb_container.clear()
        if is_video:
            self.cb_container.addItems(["mp4", "mkv", "webm", "avi"])
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
        
        origem_texto = "Desconhecida (Áudio/DRM)"
        if getattr(self._current_meta, 'width', None) and getattr(self._current_meta, 'height', None):
            origem_texto = f"{self._current_meta.width}x{self._current_meta.height} @ {getattr(self._current_meta, 'fps', 'N/A')}fps"

        canal_formatado = getattr(self._current_meta, 'channel', "N/A")
        if not canal_formatado:
            canal_formatado = "N/A"
        
        base_info = (
            f"<b>Duração:</b> {getattr(self._current_meta, 'display_duration', 'N/A')}<br>"
            f"<b>Origem:</b> {origem_texto}<br>"
            f"<b>Canal:</b> {canal_formatado}"
        )
        self.stats_lbl.setText(base_info)

    def get_config_delta(self) -> Dict[str, Any]:
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
        
        self._current_meta: Optional[proc.NormalizedMediaEntity] = None

        self._analysis_cover_path: Optional[Path] = None
        self._custom_cover_path: Optional[Path] = None

        self.debounce_timer = QTimer(self)
        self.debounce_timer.setSingleShot(True)
        self.debounce_timer.setInterval(750)
        self.debounce_timer.timeout.connect(self._process_reactive_url)
        
        self.qt_log_handler = QtLogHandler()
        logging.getLogger().addHandler(self.qt_log_handler)
        
        self.init_ui()
        self._init_menu_bar()
        self._apply_theme(is_dark=True)

    def _check_cookie_format(self) -> bool:
        cookie_path = Path("cookies.txt")
        if not cookie_path.exists():
            return True
            
        try:
            with open(cookie_path, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read(100)
                if "# Netscape HTTP Cookie File" not in content:
                    QMessageBox.critical(
                        self,
                        "Erro de Integridade Léxica (Cookies)",
                        "O ficheiro 'cookies.txt' foi detetado no diretório raiz, contudo, "
                        "não obedece ao padrão 'Netscape HTTP Cookie File'.\n\n"
                        "A ausência do cabeçalho causará uma exceção (LoadError) no motor yt-dlp.\n\n"
                        "Mitigação Recomendada:\n"
                        "Gere o ficheiro utilizando a extensão de navegador 'Get cookies.txt LOCALLY'."
                    )
                    return False
        except Exception as e:
            logging.warning(f"[I/O] Restrição ao validar heurística de cookies: {e}")
            
        return True

    @pyqtSlot(str)
    def _on_url_text_changed(self, text: str) -> None:
        self.debounce_timer.start()

    @pyqtSlot()
    def _process_reactive_url(self) -> None:
        url = self.url_input.text().strip()
        
        if not url or not proc.YtDlpService.validate_url(url):
            self._reset_ui_state()
            return

        self.start_analysis()

    def _reset_ui_state(self) -> None:
        if not sip.isdeleted(self.inspector):
            self.inspector.clear_thumbnail()
            self.inspector.setVisible(False)
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(False)
        if not sip.isdeleted(self.btn_analyze):
            self.btn_analyze.setEnabled(True)
            self.btn_analyze.setText("Analisar Multimédia")
        self._current_meta = None
        self._analysis_cover_path = None
        self._custom_cover_path = None

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
        
        self.url_input.textChanged.connect(self._on_url_text_changed)
        
        self.btn_analyze = QPushButton("Analisar Multimédia", input_frame)
        self.btn_analyze.setObjectName("PrimaryAction")
        self.btn_analyze.setFixedHeight(40)
        self.btn_analyze.clicked.connect(self.start_analysis)
        input_layout.addWidget(self.url_input, 1)
        input_layout.addWidget(self.btn_analyze)
        main_layout.addWidget(input_frame)

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
        
        if not self._check_cookie_format():
            self._reset_ui_state()
            return
        
        if not sip.isdeleted(self.inspector):
            self.inspector.clear_thumbnail()
        
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
    def on_analysis_success(self, meta: proc.NormalizedMediaEntity) -> None:
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
        
        import tempfile
        cache_dir = Path(tempfile.gettempdir())
        self._analysis_cover_path = cache_dir / f"cover_sndstream_yt_{uuid.uuid4().hex[:8]}.jpg"
        
        pixmap = QPixmap()
        if pixmap.loadFromData(data):
            safe_image = pixmap.toImage().convertToFormat(QImage.Format.Format_RGB32)
            safe_image.save(str(self._analysis_cover_path), "JPG", 95)
        
        if not getattr(self, '_custom_cover_path', None):
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
        if not self._check_cookie_format(): return
        
        data = self.inspector.get_config_delta()
        if not data: return
        
        entities_to_process: List[proc.NormalizedMediaEntity] = (
            self._current_meta.children if getattr(self._current_meta, 'is_playlist', False) and self._current_meta.children
            else [self._current_meta]
        )

        for entity in entities_to_process:
            job_id = str(uuid.uuid4())
            target_url = str(getattr(entity, 'original_id', ''))
            is_search = getattr(entity, 'is_search_query', False)
            
            current_media_type = data['media_type']
            current_format = data['format_container']
            
            if is_search:
                current_media_type = proc.MediaType.AUDIO
                if current_format in ['mp4', 'mkv', 'webm', 'avi']:
                    current_format = 'mp3' 
            
            if is_search or target_url.startswith("ytmsearch") or target_url.startswith("ytsearch"):
                query = target_url.split(":", 1)[-1] if ":" in target_url else target_url
                
                target_url = f'ytsearch1:{query.strip()} "Provided to YouTube"'
                
            elif not target_url.startswith("http") and not target_url.startswith("ytsearch"):
                target_url = f"https://www.youtube.com/watch?v={target_url}"

            resolved_filename = data.get('custom_filename', 'output')
            if getattr(self._current_meta, 'is_playlist', False):
                safe_title = re.sub(r'[<>:"/\\|?*]', '', f"{entity.artist} - {entity.title}")
                resolved_filename = safe_title.strip()

            config = proc.DownloadJobConfig(
                job_id=job_id,
                url=target_url,
                output_path=Path(self.path_input.text()),
                media_type=current_media_type,
                format_container=current_format,
                audio_codec=data['audio_codec'],
                video_codec=data['video_codec'],
                quality_preset=data['quality_preset'],
                audio_sample_rate=data['audio_sample_rate'],
                audio_bitrate=str(data['audio_bitrate']),
                custom_filename=resolved_filename,
                meta_title=entity.title,
                meta_artist=entity.artist,
                meta_album=entity.album,
                meta_genre=data.get('meta_genre', ''),
                meta_date=entity.upload_date if getattr(entity, 'upload_date', None) else data.get('meta_date', ''),
                meta_desc=entity.description if getattr(entity, 'description', None) else data.get('meta_desc', ''),
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

            object.__setattr__(config, "spotify_thumb_url", getattr(entity, 'thumbnail_url', None))

            final_cover = self._custom_cover_path if self._custom_cover_path else self._analysis_cover_path
            object.__setattr__(config, "custom_cover_path", str(final_cover) if final_cover else "")

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