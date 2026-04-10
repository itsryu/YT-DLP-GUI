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
import subprocess
import shutil
import tempfile
import copy
from dataclasses import dataclass, field, replace
from typing import Callable, Final, Dict, Any, Optional, List, TypeVar, Set
from pathlib import Path
from functools import wraps
import shlex

from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QGridLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QFileDialog,
    QTableWidget, QTableWidgetItem, QHeaderView, QProgressBar,
    QMessageBox, QFrame, QScrollArea, QGroupBox, QFormLayout, 
    QCheckBox, QPlainTextEdit, QSplitter, QTabWidget, QRadioButton, 
    QButtonGroup, QAbstractItemView, QMenu, QDialog, QDialogButtonBox,
    QTableView
)
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThreadPool, pyqtSlot, QUrl, QRunnable, QTimer, QAbstractTableModel, QModelIndex, QSettings
from PyQt6.QtGui import QColor, QPixmap, QFont, QTextCursor, QTextCharFormat, QDesktopServices, QPalette, QAction, QCloseEvent, QImage
from PyQt6 import sip

import processamento as proc

PROJECT_DIR = str(Path(__file__).parent.absolute())
if PROJECT_DIR not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"{PROJECT_DIR};{os.environ.get('PATH', '')}"

T = TypeVar('T')

APP_NAME: Final[str] = "SoundStream Pro"
VERSION: Final[str] = "7.10.3"
DEFAULT_DOWNLOAD_DIR: Final[Path] = Path.home() / "Downloads"
MAX_CONCURRENT_DOWNLOADS: Final[int] = 3

MB_CONTACT = os.environ.get("MB_CONTACT_EMAIL", "contact@soundstream.app")
MB_USER_AGENT = f"{APP_NAME}/{VERSION} ( {MB_CONTACT} )"

_SHARED_SSL_CTX: ssl.SSLContext = ssl.create_default_context()

@dataclass(frozen=True)
class EntityStub:
    original_id: str = ''
    title: str = ''
    artist: str = ''
    album: str = ''
    duration: float = 0.0
    filesize: int = 0
    is_playlist: bool = False
    children: List[Any] = field(default_factory=list)
    is_search_query: bool = False
    width: Optional[int] = None
    height: Optional[int] = None
    fps: Optional[float] = None
    channel: str = 'Desconhecido'
    upload_date: str = ''
    description: str = ''
    thumbnail_url: Optional[str] = None
    genre: str = ''

    @property
    def display_duration(self) -> str:
        safe_duration = int(float(self.duration)) if self.duration else 0
        if safe_duration <= 0: return "N/A"
        minutes, seconds = divmod(safe_duration, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours > 0 else f"{minutes:02d}:{seconds:02d}"

@dataclass
class JobRecord:
    config: proc.DownloadJobConfig
    state: proc.DownloadJobState
    runnable: Optional[proc.DownloadWorker] = None
    is_terminal: bool = False

def with_exponential_backoff(max_retries: int = 3, base_delay: float = 1.0) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
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

class UrlResolver:
    @staticmethod
    def resolve_download_url(entity: Any, target_url: str) -> str:
        if getattr(entity, 'is_search_query', False) or target_url.startswith("ytmsearch") or target_url.startswith("ytsearch"):
            query = target_url.split(":", 1)[-1] if ":" in target_url else target_url
            return f'ytsearch5:{query.strip()} "Provided to YouTube"'
        elif not target_url.startswith("http") and not target_url.startswith("ytsearch"):
            return f"https://www.youtube.com/watch?v={target_url}"
        return target_url

class CookieImportSignals(QObject):
    success = pyqtSignal(str)
    error = pyqtSignal(str)

class CookieImportWorker(QRunnable):
    def __init__(self, source_path: str) -> None:
        super().__init__()
        self.source_path = Path(source_path)
        self.signals = CookieImportSignals()

    @pyqtSlot()
    def run(self) -> None:
        try:
            with open(self.source_path, "r", encoding="utf-8", errors="ignore") as f:
                if "# Netscape HTTP Cookie File" not in f.read(120):
                    self.signals.error.emit("A assinatura léxica do ficheiro não corresponde à RFC 'Netscape HTTP Cookie File'. A importação foi abortada.")
                    return
            
            target_path = Path.cwd() / "cookies.txt"
            shutil.copy2(self.source_path, target_path)
            self.signals.success.emit(str(target_path.absolute()))
        except Exception as e:
            self.signals.error.emit(f"Falha de I/O não tratada: {str(e)}")

class MetadataDSPSignals(QObject):
    success = pyqtSignal(Path)
    error = pyqtSignal(str)

class MetadataDSPWorker(QRunnable):
    def __init__(self, cmd: List[str], temp_out: Path, target_filepath: Path, original_filepath: Path) -> None:
        super().__init__()
        self.cmd = cmd
        self.temp_out = temp_out
        self.target_filepath = target_filepath
        self.original_filepath = original_filepath
        self.signals = MetadataDSPSignals()

    @pyqtSlot()
    def run(self) -> None:
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
        try:
            file_size = self.original_filepath.stat().st_size
            is_stream_copy = "-c copy" in " ".join(self.cmd)
            timeout_sec = 300 if is_stream_copy else max(120, int(file_size / (512 * 1024)))
            
            subprocess.run(self.cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, startupinfo=startupinfo, timeout=timeout_sec)
            
            if self.target_filepath.exists() and self.target_filepath.absolute() != self.original_filepath.absolute():
                self.target_filepath.unlink(missing_ok=True)
                
            shutil.move(str(self.temp_out), str(self.target_filepath))
            
            if self.target_filepath.absolute() != self.original_filepath.absolute():
                self.original_filepath.unlink(missing_ok=True)
                
            self.signals.success.emit(self.target_filepath)
        except subprocess.TimeoutExpired:
            if self.temp_out.exists(): self.temp_out.unlink()
            self.signals.error.emit("A transcodificação atómica excedeu o tempo limite.")
        except subprocess.CalledProcessError as e:
            if self.temp_out.exists(): self.temp_out.unlink()
            self.signals.error.emit(f"Kernel Panic (FFmpeg):\n{e.stderr}")
        except Exception as e:
            if self.temp_out.exists(): self.temp_out.unlink()
            self.signals.error.emit(str(e))

class LocalMetadataEditorDialog(QDialog):
    def __init__(self, filepath: str, initial_data: Dict[str, str], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.filepath: Path = Path(filepath)
        self.initial_data: Dict[str, str] = initial_data
        self.new_filepath: Optional[Path] = None
        self.setWindowTitle("Editor Transacional de Metadados (FFmpeg DSP)")
        self.setMinimumSize(550, 450)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        
        content = QWidget()
        form = QFormLayout(content)
        
        self.in_filename = QLineEdit(self.filepath.stem)
        self.in_title = QLineEdit(self.initial_data.get('meta_title', ''))
        self.in_artist = QLineEdit(self.initial_data.get('meta_artist', ''))
        self.in_album = QLineEdit(self.initial_data.get('meta_album', ''))
        self.in_genre = QLineEdit(self.initial_data.get('meta_genre', ''))
        self.in_date = QLineEdit(self.initial_data.get('meta_date', ''))
        self.in_desc = QPlainTextEdit(self.initial_data.get('meta_desc', ''))
        self.in_desc.setFixedHeight(60)
        
        form.addRow("Nome do Ficheiro:", self.in_filename)
        form.addRow("Título:", self.in_title)
        form.addRow("Artista:", self.in_artist)
        form.addRow("Álbum:", self.in_album)
        form.addRow("Género:", self.in_genre)
        form.addRow("Data (Ano):", self.in_date)
        form.addRow("Descrição:", self.in_desc)
        
        scroll.setWidget(content)
        layout.addWidget(scroll)
        
        lbl_info = QLabel("Nota: A reescrita efetua-se via <b>Stream Copy</b> atómico em background.")
        lbl_info.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(lbl_info)
        
        self.btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        self.btn_box.accepted.connect(self._apply_metadata)
        self.btn_box.rejected.connect(self.reject)
        layout.addWidget(self.btn_box)

    def _apply_metadata(self) -> None:
        if not self.filepath.exists():
            QMessageBox.critical(self, "Falha de Consistência I/O", "O ficheiro matriz não se encontra acessível no disco para leitura.")
            self.reject()
            return
            
        new_stem = re.sub(r'[<>:"/\\|?*]', '', self.in_filename.text()).strip() or "output_modificado"
        target_filepath = self.filepath.with_name(f"{new_stem}{self.filepath.suffix}")
        temp_out = self.filepath.with_suffix(f".temp_{uuid.uuid4().hex[:6]}{self.filepath.suffix}")
        
        cmd = ["ffmpeg", "-y", "-i", str(self.filepath), "-c", "copy"]
        
        def add_meta(key: str, val: str) -> None:
            val_clean = val.replace('"', "'").strip()
            cmd.extend(["-metadata", f"{key}={val_clean}"])
            
        add_meta("title", self.in_title.text())
        add_meta("artist", self.in_artist.text())
        add_meta("album_artist", self.in_artist.text())
        add_meta("album", self.in_album.text())
        add_meta("genre", self.in_genre.text())
        add_meta("date", self.in_date.text()[:4])
        add_meta("comment", self.in_desc.toPlainText())
        add_meta("description", self.in_desc.toPlainText())
        cmd.append(str(temp_out))
        
        self.btn_box.setEnabled(False)
        worker = MetadataDSPWorker(cmd, temp_out, target_filepath, self.filepath)
        worker.signals.success.connect(self._on_success)
        worker.signals.error.connect(self._on_error)
        QThreadPool.globalInstance().start(worker)

    @pyqtSlot(Path)
    def _on_success(self, target: Path) -> None:
        self.new_filepath = target
        QMessageBox.information(self, "Transação Concluída", "Mutação de metadados consolidada com êxito.")
        self.accept()

    @pyqtSlot(str)
    def _on_error(self, err: str) -> None:
        self.btn_box.setEnabled(True)
        QMessageBox.critical(self, "Erro I/O", err)

@dataclass(frozen=True)
class EngineFlag:
    cli_arg: str
    description: str
    requires_input: bool = False
    category: str = "Geral"

class EngineFlagsDialog(QDialog):
    def __init__(self, current_flags: str, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Configuração Avançada de Parâmetros (CLI)")
        self.setMinimumSize(650, 500)
        
        self._current_flags: str = current_flags
        self._flag_registry: List[EngineFlag] = self._initialize_registry()
        self._ui_elements: Dict[str, tuple[QCheckBox, Optional[QLineEdit]]] = {}
        
        self._init_ui()
        self._hydrate_state()

    def _initialize_registry(self) -> List[EngineFlag]:
        return [
            EngineFlag("--force-ipv4", "Forçar resolução de sockets via IPv4", category="Rede e Transporte"),
            EngineFlag("--force-ipv6", "Forçar resolução de sockets via IPv6", category="Rede e Transporte"),
            EngineFlag("--limit-rate", "Limitar largura de banda (ex: 50K, 2M)", requires_input=True, category="Rede e Transporte"),
            EngineFlag("--proxy", "URI de Proxy HTTP/SOCKS", requires_input=True, category="Rede e Transporte"),
            EngineFlag("--socket-timeout", "Tempo limite de resposta (Time-to-Live) em segundos", requires_input=True, category="Rede e Transporte"),
            EngineFlag("--geo-bypass", "Contornar restrições geográficas via cabeçalhos injetados", category="Evasão de Restrições"),
            EngineFlag("--cookies-from-browser", "Extrair matriz de estado (Cookies) do navegador", requires_input=True, category="Evasão de Restrições"),
            EngineFlag("--user-agent", "Falsificação estrita da string de User-Agent", requires_input=True, category="Evasão de Restrições"),
            EngineFlag("--sleep-requests", "Atraso determinístico entre requisições iterativas (segundos)", requires_input=True, category="Regulação de Fluxo"),
            EngineFlag("--sleep-interval", "Atraso limite inferior (randômico) entre transações", requires_input=True, category="Regulação de Fluxo"),
            EngineFlag("--max-sleep-interval", "Atraso limite superior (randômico) entre transações", requires_input=True, category="Regulação de Fluxo"),
            EngineFlag("--ignore-errors", "Ignorar exceções isoladas e manter topologia contínua", category="Sistema de Arquivos"),
            EngineFlag("--no-warnings", "Suprimir pipeline de avisos no STDERR", category="Sistema de Arquivos"),
            EngineFlag("--restrict-filenames", "Normalizar nomenclatura para o padrão ASCII", category="Sistema de Arquivos"),
            EngineFlag("--windows-filenames", "Garantir conversão de nomenclatura para conformidade POSIX/Win32", category="Sistema de Arquivos"),
            EngineFlag("--no-overwrites", "Bloquear sobrescrita (Skip) em partições alocadas", category="Sistema de Arquivos"),
            EngineFlag("--continue", "Forçar a retoma explícita de blocos binários", category="Sistema de Arquivos"),
            EngineFlag("--match-filter", "Filtro booleano AST", requires_input=True, category="Processamento Estrutural"),
            EngineFlag("--playlist-reverse", "Inverter a fila do algoritmo de busca (LIFO)", category="Processamento Estrutural"),
            EngineFlag("--break-on-existing", "Interromper Thread ao encontrar nó persistido", category="Processamento Estrutural"),
            EngineFlag("--max-downloads", "Limite quantitativo absoluto de nós a extrair", requires_input=True, category="Processamento Estrutural"),
            EngineFlag("--write-subs", "Efetuar I/O de legendas nativas", category="Metadados e Telemetria"),
            EngineFlag("--write-auto-subs", "Sintetizar matriz de legendas automáticas (ASR)", category="Metadados e Telemetria"),
            EngineFlag("--sub-langs", "Vetor ISO de segmentação de idiomas (ex: en,pt)", requires_input=True, category="Metadados e Telemetria"),
            EngineFlag("--embed-chapters", "Injetar matriz estrutural de capítulos via multiplexador", category="Metadados e Telemetria"),
            EngineFlag("--write-info-json", "Descarregar manifesto RAW (JSON)", category="Metadados e Telemetria"),
            EngineFlag("--extractor-args", "Injeção nativa de dependências lógicas ao módulo base", requires_input=True, category="Baixo Nível")
        ]

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)
        container = QWidget()
        container_layout = QVBoxLayout(container)
        
        categories: Dict[str, List[EngineFlag]] = {}
        for flag in self._flag_registry:
            categories.setdefault(flag.category, []).append(flag)

        for category, flags in categories.items():
            group = QGroupBox(category, container)
            group_layout = QVBoxLayout(group)
            
            for flag in flags:
                row_layout = QHBoxLayout()
                chk = QCheckBox(f"{flag.cli_arg} - {flag.description}", group)
                input_field = None
                if flag.requires_input:
                    input_field = QLineEdit(group)
                    input_field.setPlaceholderText("Atribuir valor...")
                    input_field.setEnabled(False)
                    def toggle_input(state: int, field: QLineEdit = input_field) -> None:
                        field.setEnabled(state == Qt.CheckState.Checked.value)
                    chk.stateChanged.connect(toggle_input)
                row_layout.addWidget(chk)
                if input_field:
                    row_layout.addWidget(input_field)
                group_layout.addLayout(row_layout)
                self._ui_elements[flag.cli_arg] = (chk, input_field)
            container_layout.addWidget(group)

        unknown_group = QGroupBox("Flags Personalizadas Órfãs", container)
        unknown_layout = QVBoxLayout(unknown_group)
        self.in_unknown_flags = QLineEdit(unknown_group)
        self.in_unknown_flags.setPlaceholderText("Insira parâmetros adicionais...")
        unknown_layout.addWidget(self.in_unknown_flags)
        container_layout.addWidget(unknown_group)

        container_layout.addStretch()
        scroll.setWidget(container)
        layout.addWidget(scroll)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.button(QDialogButtonBox.StandardButton.Ok).setText("Aplicar")
        btn_box.button(QDialogButtonBox.StandardButton.Cancel).setText("Cancelar")
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

    def _hydrate_state(self) -> None:
        if not self._current_flags: return
        try:
            tokens = shlex.split(self._current_flags)
        except ValueError as e:
            logging.warning(f"[Lexer] Falha ao efetuar parse da sintaxe AST: {e}")
            tokens = self._current_flags.split()

        unknown = []
        i = 0
        while i < len(tokens):
            token = tokens[i]
            if token in self._ui_elements:
                chk, input_field = self._ui_elements[token]
                chk.setChecked(True)
                flag = next((f for f in self._flag_registry if f.cli_arg == token), None)
                if flag and flag.requires_input and input_field:
                    if i + 1 < len(tokens) and not tokens[i+1].startswith("--"):
                        input_field.setText(tokens[i+1])
                        i += 1
            else:
                unknown.append(token)
            i += 1
        
        if unknown:
            self.in_unknown_flags.setText(" ".join(unknown))

    def compile_flags(self) -> str:
        compiled = []
        for cli_arg, (chk, input_field) in self._ui_elements.items():
            if chk.isChecked():
                if input_field:
                    val = input_field.text().strip()
                    if val:
                        val_clean = val.replace('"', '\\"')
                        compiled.append(f'{cli_arg} "{val_clean}"')
                else:
                    compiled.append(cli_arg)
        
        unknown = self.in_unknown_flags.text().strip()
        if unknown:
            compiled.append(unknown)
                    
        return " ".join(compiled)

class DialogThumbnailSignals(QObject):
    ready = pyqtSignal(int, QImage)

class DialogThumbnailWorker(QRunnable):
    def __init__(self, row_index: int, release_id: str, release_group_id: str) -> None:
        super().__init__()
        self.row_index = row_index
        self.release_id = release_id
        self.release_group_id = release_group_id
        self.signals = DialogThumbnailSignals()
        self.headers = {"User-Agent": MB_USER_AGENT, "Connection": "close"}

    @pyqtSlot()
    def run(self) -> None:
        endpoints = []
        if self.release_id: endpoints.append(f"https://coverartarchive.org/release/{self.release_id}/front-250")
        if self.release_group_id: endpoints.append(f"https://coverartarchive.org/release-group/{self.release_group_id}/front-250")

        for url in endpoints:
            try:
                req = urllib.request.Request(url, headers=self.headers)
                with urllib.request.urlopen(req, timeout=8, context=_SHARED_SSL_CTX) as response:
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
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.last_update = time.monotonic()
        self.condition = threading.Condition()

    def wait(self) -> None:
        with self.condition:
            def _has_token() -> bool:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(float(self.capacity), self.tokens + elapsed * self.rate)
                self.last_update = now
                return self.tokens >= 1.0

            self.condition.wait_for(_has_token)
            self.tokens -= 1.0
            self.condition.notify_all()

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
            QWidget {{ font-family: 'Segoe UI', 'Roboto', sans-serif; font-size: 13px; }}
            QFrame#Panel {{ border-radius: 8px; border: 1px solid {BORDER}; background-color: {PANEL_BG}; }}
            QPushButton#PrimaryAction {{ background-color: #007acc; color: white; border-radius: 4px; padding: 8px 16px; font-weight: bold; border: none; }}
            QPushButton#PrimaryAction:hover {{ background-color: #0062a3; }}
            QPushButton#PrimaryAction:disabled {{ background-color: {DISABLED_BG}; color: {DISABLED_TXT}; }}
            QPushButton#Destructive {{ color: #d32f2f; border: 1px solid #d32f2f; border-radius: 4px; padding: 4px 8px; background-color: transparent; }}
            QPushButton#Destructive:hover {{ background-color: rgba(211, 47, 47, 0.1); }}
            QLabel#MainHeader {{ color: #007acc; font-size: 24px; font-weight: 800; letter-spacing: 2px; }}
            QLabel#ThumbLabel {{ background-color: {THUMB_BG}; border: 1px solid {BORDER}; color: {THUMB_TEXT}; }}
            QLabel#StatsLabel, QLabel#LogHeader {{ color: {THUMB_TEXT}; }}
            QLabel#LogHeader {{ font-weight: bold; font-size: 10px; letter-spacing: 1px; }}
            QPlainTextEdit#LogConsole {{ background-color: {CONSOLE_BG}; color: {CONSOLE_TEXT}; font-family: 'Consolas', 'Courier New', monospace; font-size: 11px; border: 1px solid {BORDER}; border-radius: 4px; }}
            QMenu {{ background-color: {PANEL_BG}; border: 1px solid {BORDER}; padding: 5px; }}
            QMenu::item {{ padding: 6px 20px 6px 20px; border-radius: 4px; }}
            QMenu::item:selected {{ background-color: #007acc; color: white; }}
            QMenu::separator {{ height: 1px; background: {BORDER}; margin: 4px 10px; }}
            QLineEdit[readOnly="true"] {{ background-color: {DISABLED_BG}; color: {DISABLED_TXT}; border: 1px dashed {BORDER}; }}
        """
        tokens = {
            'BORDER': '#3d3d3d' if is_dark else '#cccccc',
            'PANEL_BG': '#1e1e1e' if is_dark else '#ffffff',
            'THUMB_BG': '#000000' if is_dark else '#eaeaea',
            'THUMB_TEXT': '#aaaaaa' if is_dark else '#666666',
            'CONSOLE_BG': '#0e0e0e' if is_dark else '#ffffff',
            'CONSOLE_TEXT': '#d4d4d4' if is_dark else '#333333',
            'DISABLED_BG': '#2a2a2a' if is_dark else '#f0f0f0',
            'DISABLED_TXT': '#888888' if is_dark else '#777777',
        }
        return qss.format_map(tokens)

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

class PlaylistTableModel(QAbstractTableModel):
    def __init__(self, entities: List[Any], parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._entities = entities
        self._checked_states = [True] * len(entities)
        self._headers = ["Inc.", "Título", "Artista", "Álbum", "Duração"]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._entities)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._headers)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid(): return None
        row, col = index.row(), index.column()
        entity = self._entities[row]

        if role == Qt.ItemDataRole.DisplayRole:
            if col == 1: return entity.title
            if col == 2: return entity.artist
            if col == 3: return entity.album
            if col == 4: return entity.display_duration
        elif role == Qt.ItemDataRole.CheckStateRole and col == 0:
            return Qt.CheckState.Checked.value if self._checked_states[row] else Qt.CheckState.Unchecked.value
        elif role == Qt.ItemDataRole.TextAlignmentRole and col in (0, 4):
            return Qt.AlignmentFlag.AlignCenter.value
        return None

    def setData(self, index: QModelIndex, value: Any, role: int = Qt.ItemDataRole.EditRole) -> bool:
        if role == Qt.ItemDataRole.CheckStateRole and index.column() == 0:
            self._checked_states[index.row()] = (value == Qt.CheckState.Checked.value)
            self.dataChanged.emit(index, index, [role])
            return True
        return False

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        flags = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
        if index.column() == 0:
            flags |= Qt.ItemFlag.ItemIsUserCheckable
        return flags

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._headers[section]
        return None

    def toggle_all(self, state: bool) -> None:
        self._checked_states = [state] * len(self._entities)
        if self._entities:
            top_left = self.index(0, 0)
            bottom_right = self.index(len(self._entities) - 1, 0)
            self.dataChanged.emit(top_left, bottom_right, [Qt.ItemDataRole.CheckStateRole])

    def get_selected_entities(self) -> List[Any]:
        return [ent for i, ent in enumerate(self._entities) if self._checked_states[i]]
    
class PlaylistStagingDialog(QDialog):
    def __init__(self, entities: List[Any], parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Pré-Processamento de Topologia em Árvore (Playlist)")
        self.setMinimumSize(900, 500)
        self.model = PlaylistTableModel(entities, self)
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        lbl_info = QLabel(f"<b>Topologia detectada:</b> Lista de Reprodução ou Álbum ({self.model.rowCount()} nós identificados).<br>Selecione as entidades a transacionar para o motor DSP.")
        layout.addWidget(lbl_info)
        
        self.view = QTableView(self)
        self.view.setModel(self.model)
        self.view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.view.verticalHeader().setVisible(False)
        
        header = self.view.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        
        layout.addWidget(self.view)
        
        ctrl_layout = QHBoxLayout()
        btn_sel_all = QPushButton("Selecionar Todos")
        btn_sel_none = QPushButton("Remover Seleção")
        
        btn_sel_all.clicked.connect(lambda: self.model.toggle_all(True))
        btn_sel_none.clicked.connect(lambda: self.model.toggle_all(False))
        
        ctrl_layout.addWidget(btn_sel_all)
        ctrl_layout.addWidget(btn_sel_none)
        ctrl_layout.addStretch()
        
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        btn_box.button(QDialogButtonBox.StandardButton.Ok).setText("Confirmar Triagem")
        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        
        ctrl_layout.addWidget(btn_box)
        layout.addLayout(ctrl_layout)

    def get_selected_entities(self) -> List[Any]:
        return self.model.get_selected_entities()

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
    def __init__(self, title_query: str, artist_query: str, album_query: str, date_query: str, user_agent: str) -> None:
        super().__init__()
        self.title_query = title_query
        self.artist_query = artist_query
        self.album_query = album_query
        self.date_query = date_query
        self.user_agent = user_agent
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
                "User-Agent": self.user_agent,
                "Accept": "application/json",
                "Connection": "keep-alive"
            }
            req = urllib.request.Request(url, headers=headers)
            
            mb_rate_limiter.wait()
            logging.debug(f"[Network] A consultar AST Lucene MusicBrainz: {url}")
            
            with urllib.request.urlopen(req, timeout=15, context=_SHARED_SSL_CTX) as response:
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
        self.release_id = release_id
        self.release_group_id = release_group_id
        self.signals = CoverArtSignals()
        self.headers = {
            "User-Agent": MB_USER_AGENT,
            "Accept": "application/json",
            "Connection": "keep-alive"
        }

    @with_exponential_backoff(max_retries=5, base_delay=1.0)
    def _fetch_json_manifest(self, endpoint_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        if not entity_id: return None
        
        url = f"https://coverartarchive.org/{endpoint_type}/{entity_id}"
        req = urllib.request.Request(url, headers=self.headers)
        
        mb_rate_limiter.wait()
        logging.debug(f"[Network] A analisar manifesto do {endpoint_type}: {entity_id}")
        
        try:
            with urllib.request.urlopen(req, timeout=15, context=_SHARED_SSL_CTX) as response:
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
        if not images: return None
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
                manifest = self._fetch_json_manifest("release-group", self.release_group_id)

            if not manifest:
                self.signals.error.emit("A entidade requisitada e o seu grupo não possuem metadados visuais arquivados.")
                return

            optimal_url = self._extract_optimal_resolution(manifest)
            if not optimal_url:
                self.signals.error.emit("Nenhum nó de imagem front/thumbnail classificado no manifesto.")
                return

            req = urllib.request.Request(optimal_url, headers={"User-Agent": self.headers["User-Agent"]})
            with urllib.request.urlopen(req, timeout=30, context=_SHARED_SSL_CTX) as response:
                buffer = response.read()

            image = QImage()
            if image.loadFromData(buffer):
                self.signals.result_ready.emit(image)
            else:
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
            50, 50, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation
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
        self._current_meta: Optional[EntityStub] = None
        self._current_source_url: str = ""
        self._local_custom_cover_path: str = ""
        self._filename_debounce = QTimer(self)
        self._filename_debounce.setSingleShot(True)
        self._filename_debounce.setInterval(150)
        self._filename_debounce.timeout.connect(self._compute_dynamic_filename)
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
        
        self.btn_custom_cover = QPushButton("Alterar Capa", left_col)
        self.btn_custom_cover.clicked.connect(self._browse_custom_cover)
        
        left_layout.addWidget(self.thumb_lbl)
        left_layout.addWidget(self.stats_lbl)
        left_layout.addWidget(self.btn_custom_cover)
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
        self.cb_container.currentTextChanged.connect(self._trigger_filename_update)
        
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
        self.cb_quality.currentTextChanged.connect(self._recalc_estimate)
        
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
        self.in_filename.setReadOnly(True)
        
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
        
        cookie_layout = QHBoxLayout()
        cookie_layout.setContentsMargins(0, 0, 0, 0)
        self.chk_cookies = QCheckBox("Utilizar Cookies do Navegador", chk_group)
        self.btn_import_cookies = QPushButton("Importar", chk_group)
        self.btn_import_cookies.clicked.connect(self._import_cookies)
        
        cookie_layout.addWidget(self.chk_cookies)
        cookie_layout.addWidget(self.btn_import_cookies)
        cookie_layout.addStretch()
        
        self.chk_meta.setChecked(True)
        self.chk_thumb.setChecked(True)
        
        chk_layout.addWidget(self.chk_meta)
        chk_layout.addWidget(self.chk_thumb)
        chk_layout.addWidget(self.chk_subs)
        chk_layout.addWidget(self.chk_norm)
        chk_layout.addLayout(cookie_layout)
        
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
        self.in_custom_flags.setReadOnly(True)
        
        self.btn_open_flags = QPushButton("Personalizar Flags", dev_group)
        self.btn_open_flags.clicked.connect(self._open_flags_editor)
        
        flags_layout = QHBoxLayout()
        flags_layout.setContentsMargins(0, 0, 0, 0)
        flags_layout.addWidget(self.in_custom_flags)
        flags_layout.addWidget(self.btn_open_flags)
        
        dev_form.addRow("Template de Saída:", tmpl_layout)
        dev_form.addRow("Caminho FFmpeg:", ffmpeg_layout)
        dev_form.addRow("Flags Personalizadas:", flags_layout)
        
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
        
        self.in_title.textChanged.connect(self._trigger_filename_update)
        self.in_artist.textChanged.connect(self._trigger_filename_update)
        self.in_album.textChanged.connect(self._trigger_filename_update)
        self.in_genre.textChanged.connect(self._trigger_filename_update)
        self.in_date.textChanged.connect(self._trigger_filename_update)
        self.in_output_tmpl.textChanged.connect(self._trigger_filename_update)

    @pyqtSlot()
    def _trigger_filename_update(self, *args: Any) -> None:
        self._filename_debounce.start()

    @pyqtSlot()
    def _open_flags_editor(self) -> None:
        if sip.isdeleted(self.in_custom_flags): return
        dialog = EngineFlagsDialog(self.in_custom_flags.text().strip(), self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.in_custom_flags.setText(dialog.compile_flags())

    def load_from_config(self, config: Any, state: Any = None) -> None:
        if getattr(config, 'media_type', None) == proc.MediaType.VIDEO:
            self.rb_video.setChecked(True)
        else:
            self.rb_audio.setChecked(True)
            
        self._update_ui_mode()

        container = getattr(config, 'format_container', '')
        if container:
            idx = self.cb_container.findText(container, Qt.MatchFlag.MatchExactly)
            if idx >= 0: self.cb_container.setCurrentIndex(idx)

        for attr, cb in [('video_codec', self.cb_vcodec), ('audio_codec', self.cb_acodec)]:
            if hasattr(config, attr):
                idx = cb.findText(getattr(config, attr), Qt.MatchFlag.MatchContains | Qt.MatchFlag.MatchCaseSensitive)
                if idx >= 0: cb.setCurrentIndex(idx)

        if hasattr(config, 'quality_preset'):
            idx = self.cb_quality.findText(config.quality_preset, Qt.MatchFlag.MatchExactly)
            if idx >= 0: self.cb_quality.setCurrentIndex(idx)

        if hasattr(config, 'audio_bitrate'):
            idx = self.cb_abitrate.findData(str(config.audio_bitrate))
            if idx >= 0: self.cb_abitrate.setCurrentIndex(idx)

        if hasattr(config, 'audio_sample_rate'):
            asr_val = "auto" if config.audio_sample_rate == 0 else str(config.audio_sample_rate)
            idx = self.cb_asr.findData(asr_val)
            if idx >= 0: self.cb_asr.setCurrentIndex(idx)

        self.in_title.setText(getattr(config, 'meta_title', ''))
        self.in_artist.setText(getattr(config, 'meta_artist', ''))
        self.in_album.setText(getattr(config, 'meta_album', ''))
        self.in_genre.setText(getattr(config, 'meta_genre', ''))
        self.in_date.setText(getattr(config, 'meta_date', ''))
        self.in_desc.setPlainText(getattr(config, 'meta_desc', ''))

        self.chk_meta.setChecked(getattr(config, 'embed_metadata', True))
        self.chk_thumb.setChecked(getattr(config, 'embed_thumbnail', True))
        self.chk_subs.setChecked(getattr(config, 'embed_subs', False))
        self.chk_norm.setChecked(getattr(config, 'normalize_audio', False))
        self.chk_cookies.setChecked(getattr(config, 'use_browser_cookies', False))

        if hasattr(config, 'output_template') and config.output_template:
            tmpl = config.output_template
            if not tmpl.endswith(".%(ext)s"): tmpl += ".%(ext)s"
            self.in_output_tmpl.setText(tmpl)

        if hasattr(config, 'ffmpeg_path'): self.in_ffmpeg_path.setText(config.ffmpeg_path)
        if hasattr(config, 'custom_flags'): self.in_custom_flags.setText(config.custom_flags)

        self._current_meta = EntityStub(
            original_id=getattr(config, 'url', ''),
            title=getattr(config, 'meta_title', ''),
            artist=getattr(config, 'meta_artist', ''),
            album=getattr(config, 'meta_album', ''),
            genre=getattr(config, 'meta_genre', ''),
            upload_date=getattr(config, 'meta_date', ''),
            description=getattr(config, 'meta_desc', ''),
            thumbnail_url=getattr(config, 'spotify_thumb_url', None)
        )
        self._current_source_url = getattr(config, 'url', '')

        cover_path = getattr(state, 'custom_cover_path', None) if state else getattr(config, 'custom_cover_path', None)
        if cover_path:
            path = Path(cover_path)
            if path.exists():
                self._local_custom_cover_path = str(path)
                self.set_thumbnail(QPixmap(str(path)))

        self._compute_dynamic_filename()

    @pyqtSlot()
    def _compute_dynamic_filename(self) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.in_output_tmpl) or sip.isdeleted(self.in_filename): return
            
        tmpl = self.in_output_tmpl.text().strip()
        if not tmpl:
            self.in_filename.setText("")
            self.in_output_tmpl.setStyleSheet("border: 1px solid #d32f2f;")
            return
            
        self.in_output_tmpl.setStyleSheet("")
        is_batch = not self.tabs.isTabEnabled(1)
        
        mapping = {
            'title': self.in_title.text().strip() or ("Título" if not is_batch else "Variáveis em Lote"),
            'artist': self.in_artist.text().strip() or ("Artista" if not is_batch else "Vários Artistas"),
            'uploader': self.in_artist.text().strip() or ("Artista" if not is_batch else "Vários Artistas"),
            'album': self.in_album.text().strip() or ("Álbum" if not is_batch else "Vários Álbuns"),
            'genre': self.in_genre.text().strip() or "Género",
            'release_year': self.in_date.text().strip()[:4] if self.in_date.text().strip() else "Ano",
            'upload_date': self.in_date.text().strip() or "Data",
            'ext': self.cb_container.currentText() if not sip.isdeleted(self.cb_container) else "ext",
            'playlist': "Playlist",
            'playlist_index': "01"
        }
        
        def safe_sub(match: re.Match) -> str:
            val = mapping.get(match.group(1), f"%({match.group(1)})s")
            return re.sub(r'[<>:"/\\|?*]', '', str(val))
            
        try:
            res = re.sub(r'%\(([^)]+)\)s', safe_sub, tmpl)
            res = re.sub(r'\s+', ' ', res).strip(' -_')
            self.in_filename.setText(res)
            if "%(" in res:
                self.in_output_tmpl.setStyleSheet("border: 1px solid #ff9800;")
            else:
                self.in_output_tmpl.setStyleSheet("")
        except Exception as e:
            self.in_filename.setText("Falha Lexical")
            self.in_output_tmpl.setStyleSheet("border: 1px solid #d32f2f;")

    def _browse_custom_cover(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Selecionar Capa", "", "Imagens (*.jpg *.jpeg *.png)")
        if path:
            self._local_custom_cover_path = path
            self.set_thumbnail(QPixmap(path))

    def _import_cookies(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Selecionar Matriz de Cookies", "", "Ficheiros de Texto (*.txt);;Todos os Ficheiros (*)")
        if not path: return
        self.btn_import_cookies.setEnabled(False)
        self.btn_import_cookies.setText("A Bloquear...")
        
        worker = CookieImportWorker(path)
        worker.signals.success.connect(self._on_cookies_imported)
        worker.signals.error.connect(self._on_cookies_error)
        QThreadPool.globalInstance().start(worker)

    @pyqtSlot(str)
    def _on_cookies_imported(self, path: str) -> None:
        self.btn_import_cookies.setEnabled(True)
        self.btn_import_cookies.setText("Importar")
        self.chk_cookies.setChecked(True)
        QMessageBox.information(self, "Estado Injetado", f"O descritor de estado foi transacionado de forma atómica para o workspace:\n{path}")
        
    @pyqtSlot(str)
    def _on_cookies_error(self, err: str) -> None:
        self.btn_import_cookies.setEnabled(True)
        self.btn_import_cookies.setText("Importar")
        QMessageBox.critical(self, "I/O Error", err)

    def _trigger_musicbrainz_fetch(self) -> None:
        title, artist, album = self.in_title.text().strip(), self.in_artist.text().strip(), self.in_album.text().strip()
        if not title and not artist and not album:
            QMessageBox.information(self, "Aviso", "Preencha ao menos o 'Título', 'Artista' ou 'Álbum'.")
            return

        self.btn_fetch_mb.setEnabled(False)
        self.btn_fetch_mb.setText("A procurar...")
        worker = MusicBrainzWorker(title, artist, album, self.in_date.text().strip(), MB_USER_AGENT)
        worker.signals.results_ready.connect(self._on_musicbrainz_results)
        worker.signals.error.connect(self._on_musicbrainz_error)
        QThreadPool.globalInstance().start(worker)

    @pyqtSlot(list)
    def _on_musicbrainz_results(self, candidates: List[MetadataCandidate]) -> None:
        if sip.isdeleted(self): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        if not candidates:
            QMessageBox.information(self, "MusicBrainz", "Nenhum resultado.")
            return
            
        dialog = MetadataSelectionDialog(candidates, self)
        if dialog.exec() == QDialog.DialogCode.Accepted and dialog.selected_candidate:
            cand = dialog.selected_candidate
            if cand.title: self.in_title.setText(cand.title)
            if cand.artist: self.in_artist.setText(cand.artist)
            if cand.album: self.in_album.setText(cand.album)
            if cand.date: self.in_date.setText(cand.date)
            if cand.genre: self.in_genre.setText(cand.genre)
            
            if cand.release_id:
                self.btn_fetch_mb.setText("A transferir arte HD...")
                self.btn_fetch_mb.setEnabled(False)
                ca_worker = CoverArtWorker(cand.release_id, cand.release_group_id)
                ca_worker.signals.result_ready.connect(self._on_cover_art_ready)
                ca_worker.signals.error.connect(self._on_cover_art_error)
                QThreadPool.globalInstance().start(ca_worker)

    @pyqtSlot(QImage)
    def _on_cover_art_ready(self, image: QImage) -> None:
        if sip.isdeleted(self): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        
        self.set_thumbnail(QPixmap.fromImage(image))
        self._local_custom_cover_path = str(Path(tempfile.gettempdir()) / f"cover_custom_{uuid.uuid4().hex[:8]}.jpg")
        image.convertToFormat(QImage.Format.Format_RGB32).save(self._local_custom_cover_path, "JPG", 95)

    @pyqtSlot(str)
    def _on_cover_art_error(self, err_msg: str) -> None:
        if sip.isdeleted(self): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        logging.warning(f"Arte ignorada: {err_msg}")

    @pyqtSlot(str)
    def _on_musicbrainz_error(self, err_msg: str) -> None:
        if sip.isdeleted(self): return
        self.btn_fetch_mb.setEnabled(True)
        self.btn_fetch_mb.setText("Preenchimento Automático (MusicBrainz)")
        QMessageBox.warning(self, "Erro MusicBrainz", f"Falha na API:\n{err_msg}")

    def _show_template_tutorial(self) -> None:
        QMessageBox.information(self, "Tutorial: Templates", "Variáveis Disponíveis:\n%(title)s\n%(artist)s\n%(album)s\n%(ext)s\n%(playlist_index)s\n%(release_year)s")

    def _browse_ffmpeg(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Selecionar FFmpeg", "", "Executáveis (*.exe);;Todos os Ficheiros (*)")
        if path: self.in_ffmpeg_path.setText(path)

    def set_metadata(self, meta: Any) -> None:
        if sip.isdeleted(self): return
        self._current_meta = meta
        
        self.in_title.setText(getattr(meta, 'title', ''))
        self.in_artist.setText(getattr(meta, 'artist', ''))
        self.in_album.setText(getattr(meta, 'album', ''))
        self.in_date.setText(getattr(meta, 'upload_date', '')[:4])
        self.in_desc.setPlainText(getattr(meta, 'description', ''))
        self.in_genre.clear()
        
        is_batch = len(getattr(meta, 'children', [])) > 1
        self.tabs.setTabEnabled(1, not is_batch)
        if is_batch:
            self.in_title.clear(); self.in_artist.clear(); self.in_album.clear()
        
        orig_id = getattr(meta, 'original_id', '')
        if getattr(meta, 'is_search_query', False) or any(x in str(orig_id) for x in ['ytmsearch', 'ytsearch', 'music.youtube', 'soundcloud']):
            self.rb_audio.setChecked(True)
            self.rb_video.setEnabled(False)
        else:
            self.rb_video.setEnabled(True)

        self.btn_custom_cover.setEnabled(not getattr(meta, 'is_playlist', False))
        self._recalc_estimate()
        self._compute_dynamic_filename()

    def set_thumbnail(self, pixmap: QPixmap) -> None:
        if not sip.isdeleted(self.thumb_lbl): self.thumb_lbl.setPixmap(pixmap)

    def clear_thumbnail(self) -> None:
        if not sip.isdeleted(self.thumb_lbl):
            self.thumb_lbl.clear()
            self.thumb_lbl.setText("Sem Pré-visualização")
        self._local_custom_cover_path = ""

    def _update_ui_mode(self) -> None:
        is_video = self.rb_video.isChecked()
        self.cb_container.blockSignals(True)
        self.cb_container.clear()
        self.cb_container.addItems(["mp4", "mkv", "webm", "avi"] if is_video else ["flac", "mp3", "wav", "m4a", "opus"])
        self.cb_container.blockSignals(False)
        self.cb_container.setCurrentIndex(0)
        
        for w in [self.lbl_quality, self.cb_quality, self.lbl_vcodec, self.cb_vcodec, self.lbl_acodec, self.cb_acodec]: w.setVisible(is_video)
        for w in [self.lbl_abitrate, self.cb_abitrate, self.lbl_asr, self.cb_asr]: w.setVisible(not is_video)
        self.chk_norm.setVisible(not is_video)
        self._on_container_changed(self.cb_container.currentText())

    def _on_container_changed(self, fmt: str) -> None:
        is_lossless = fmt in ['flac', 'wav']
        self.cb_abitrate.setEnabled(not is_lossless)
        self.lbl_bitdepth.setVisible(is_lossless)
        self.cb_bitdepth.setVisible(is_lossless)
        self.lbl_abitrate.setText("Bitrate (Lossless):" if is_lossless else "Bitrate:")
        self._recalc_estimate()
        self._compute_dynamic_filename()

    def _recalc_estimate(self) -> None:
        if sip.isdeleted(self) or not self._current_meta: return
        meta = self._current_meta
        children = getattr(meta, 'children', []) or []
        is_playlist = getattr(meta, 'is_playlist', False)

        total_duration = sum(float(getattr(c, 'duration', 0) or 0) for c in children) if is_playlist else float(getattr(meta, 'duration', 0) or 0)
        total_filesize = sum(int(getattr(c, 'filesize', 0) or 0) for c in children) if is_playlist else int(getattr(meta, 'filesize', 0) or 0)

        if total_filesize <= 0 and total_duration > 0:
            if self.rb_video.isChecked():
                q = self.cb_quality.currentText().lower()
                bitrate_kbps = 15000 if '4k' in q or '2160' in q else 8000 if '1440' in q else 5000 if '1080' in q else 2500 if '720' in q else 1000 if '480' in q else 500
            else:
                bitrate_kbps = 900 if self.cb_container.currentText() == 'flac' else 1411 if self.cb_container.currentText() == 'wav' else int(self.cb_abitrate.currentData() or 192)
            total_filesize = int((bitrate_kbps * 1000 / 8) * total_duration)

        html_parts = [
            f"<b>Duração Total:</b> {int(total_duration//60)}:{int(total_duration%60):02d}",
            f"<b>Tamanho Projetado:</b> {total_filesize / (1024*1024):.2f} MB",
            f"<b>Total de Nós:</b> {len(children) if is_playlist else 1}"
        ]
        self.stats_lbl.setText("<br>".join(html_parts))

    def get_config_delta(self) -> Dict[str, Any]:
        if sip.isdeleted(self): return {}
        asr = self.cb_asr.currentData()
        bd = self.cb_bitdepth.currentText().split('-')[0]
        tmpl = self.in_output_tmpl.text().strip()
        if tmpl.endswith(".%(ext)s"): tmpl = tmpl[:-8]
        
        return {
            'media_type': proc.MediaType.VIDEO if self.rb_video.isChecked() else proc.MediaType.AUDIO,
            'format_container': self.cb_container.currentText(),
            'video_codec': self.cb_vcodec.currentText().lower(),
            'audio_codec': self.cb_acodec.currentText().lower(),
            'quality_preset': self.cb_quality.currentText(),
            'audio_bitrate': self.cb_abitrate.currentData() if self.cb_abitrate.isEnabled() else "0",
            'audio_sample_rate': 0 if asr == "auto" else int(asr),
            'audio_bit_depth': "auto" if bd == "Auto" else bd, 
            'custom_filename': "" if not self.tabs.isTabEnabled(1) else self.in_filename.text().strip(),
            'output_template': tmpl,
            'ffmpeg_path': self.in_ffmpeg_path.text().strip(),
            'custom_flags': self.in_custom_flags.text().strip(),
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
            'local_custom_cover': self._local_custom_cover_path
        }

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} {VERSION}")
        self.resize(1200, 900)
        self.setMinimumSize(1000, 700)
        
        self.thread_pool = QThreadPool.globalInstance()
        self.thread_pool.setMaxThreadCount(MAX_CONCURRENT_DOWNLOADS)
        
        self._jobs: Dict[str, JobRecord] = {}
        self._jobs_lock = threading.Lock()
        self._row_index: Dict[str, int] = {}
        self._pending_retry_id: Optional[str] = None
        
        self._current_meta: Optional[EntityStub] = None
        self._temp_files: List[Path] = []
        self._analysis_cover_path: Optional[Path] = None

        self.settings = QSettings("SoundStreamPro", "Engine")

        self.debounce_timer = QTimer(self)
        self.debounce_timer.setSingleShot(True)
        self.debounce_timer.setInterval(750)
        self.debounce_timer.timeout.connect(self._process_reactive_url)
        
        self.qt_log_handler = QtLogHandler()
        logging.getLogger().addHandler(self.qt_log_handler)
        
        self.init_ui()
        self._init_menu_bar()
        self._apply_theme(True)
        self._restore_settings()

    def _restore_settings(self) -> None:
        self.path_input.setText(self.settings.value("output_path", str(DEFAULT_DOWNLOAD_DIR)))
        default_flags = '--extractor-args "youtube:player_client=android,tv" --match-filter "!is_live" --force-ipv4'
        self.inspector.in_custom_flags.setText(self.settings.value("custom_flags", default_flags))
        self.inspector.in_output_tmpl.setText(self.settings.value("output_template", "%(title)s - %(artist)s.%(ext)s"))

    def _save_settings(self) -> None:
        self.settings.setValue("output_path", self.path_input.text())
        if not sip.isdeleted(self.inspector):
            self.settings.setValue("custom_flags", self.inspector.in_custom_flags.text())
            self.settings.setValue("output_template", self.inspector.in_output_tmpl.text())

    def _check_cookie_format(self) -> bool:
        cookie_path = Path("cookies.txt")
        if not cookie_path.exists(): return True
        try:
            with open(cookie_path, "r", encoding="utf-8", errors="ignore") as f:
                if "# Netscape HTTP Cookie File" not in f.read(100):
                    QMessageBox.critical(self, "Erro de Integridade Léxica", "O ficheiro 'cookies.txt' não obedece ao padrão Netscape HTTP.")
                    return False
        except Exception: pass
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
        self._pending_retry_id = None
        if not sip.isdeleted(self.inspector):
            self.inspector.clear_thumbnail()
            self.inspector.setVisible(False)
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(False)
            self.btn_queue.setText("Adicionar à Fila")
        if not sip.isdeleted(self.btn_analyze):
            self.btn_analyze.setEnabled(True)
            self.btn_analyze.setText("Analisar Multimédia")
        self._current_meta = None

    def _init_menu_bar(self) -> None:
        menu_bar = self.menuBar()
        if not menu_bar: return
        menu_file = menu_bar.addMenu("Ficheiro")
        action_exit = QAction("Sair", self)
        action_exit.triggered.connect(self.close)
        menu_file.addAction(action_exit)
        menu_view = menu_bar.addMenu("Ver").addMenu("Tema")
        menu_view.addAction("Claro", lambda: self._apply_theme(False))
        menu_view.addAction("Escuro", lambda: self._apply_theme(True))
        menu_help = menu_bar.addMenu("Ajuda")
        menu_help.addAction("Sobre", self._show_about_dialog)

    def _apply_theme(self, is_dark: bool) -> None:
        app = QApplication.instance()
        if app:
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
        self.path_input = QLineEdit(self.action_bar)
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
        if self.table.horizontalHeader():
            self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
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
        menu = QMenu(self)
        item = self.table.itemAt(pos)
        
        if item is not None:
            row = item.row()
            job_id = self.table.item(row, 0).data(Qt.ItemDataRole.UserRole)
            status = self.table.item(row, 2).text()
            
            if "Concluído" in status:
                menu.addAction("Editar Metadados Localmente", lambda: self._edit_local_metadata(job_id))
            menu.addAction("Remover da Fila", lambda: self._remove_from_queue(job_id))
            
            with self._jobs_lock:
                record = self._jobs.get(job_id)
                if record and record.runnable:
                    menu.addAction("Cancelar Transferência", lambda: self.cancel_job(job_id))
                elif "Erro" in status or "Cancelado" in status:
                    menu.addAction("Tentar Novamente", lambda: self.retry_job(job_id))
            menu.addSeparator()
            
        menu.addAction("Abrir Pasta de Saída", self.open_output_folder)
        menu.addAction("Limpar Concluídos", self._clear_finished_jobs)
        menu.exec(self.table.viewport().mapToGlobal(pos))

    def _edit_local_metadata(self, job_id: str) -> None:
        with self._jobs_lock:
            record = self._jobs.get(job_id)
            if not record: return
            config, state = record.config, record.state
            
        filepath = Path(state.resolved_output_path) if state.resolved_output_path else Path(config.output_path) / f"{state.custom_filename}.{config.format_container}"
        
        if not filepath.exists():
            QMessageBox.warning(self, "Aviso", f"O ficheiro não foi localizado:\n{filepath}")
            return
            
        init_data = {'meta_title': config.meta_title, 'meta_artist': config.meta_artist, 'meta_album': config.meta_album, 'meta_genre': config.meta_genre, 'meta_date': config.meta_date, 'meta_desc': config.meta_desc}
        dialog = LocalMetadataEditorDialog(str(filepath), init_data, self)
        if dialog.exec() == QDialog.DialogCode.Accepted and dialog.new_filepath:
            with self._jobs_lock:
                state.custom_filename = dialog.new_filepath.stem
                state.resolved_output_path = str(dialog.new_filepath)
                record.config = replace(config, meta_title=dialog.in_title.text(), meta_artist=dialog.in_artist.text(), meta_album=dialog.in_album.text())
            row = self.get_row_by_id(job_id)
            if row >= 0:
                self.table.item(row, 0).setText(dialog.new_filepath.name)

    def _remove_from_queue(self, job_id: str) -> None:
        with self._jobs_lock:
            record = self._jobs.pop(job_id, None)
            if record:
                if record.runnable: record.runnable.cancel()
                record.is_terminal = True
            
        row = self._row_index.pop(job_id, -1)
        if row >= 0:
            self.table.removeRow(row)
            for jid in list(self._row_index.keys()):
                if self._row_index[jid] > row: self._row_index[jid] -= 1

    def _clear_finished_jobs(self) -> None:
        to_remove = []
        for row in range(self.table.rowCount()):
            if self.table.item(row, 2) and self.table.item(row, 2).text() in ["✔ Concluído", "✘ Erro", "Cancelado"]:
                to_remove.append(self.table.item(row, 0).data(Qt.ItemDataRole.UserRole))
        for jid in reversed(to_remove):
            self._remove_from_queue(jid)

    def toggle_dev_mode(self, checked: bool) -> None:
        self.log_viewer.setVisible(checked)
        logging.getLogger().setLevel(logging.DEBUG if checked else logging.INFO)

    def start_analysis(self) -> None:
        url = self.url_input.text().strip()
        if not url or not self._check_cookie_format(): return
        
        self.inspector._current_source_url = url
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
    def on_analysis_success(self, meta: Any) -> None:
        if getattr(meta, 'is_playlist', False) and getattr(meta, 'children', []):
            dialog = PlaylistStagingDialog(meta.children, self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                selected_children = dialog.get_selected_entities()
                if not selected_children:
                    self._reset_ui_state()
                    QMessageBox.information(self, "Operação Revogada", "A transação foi abortada: Nenhum sub-nó foi selecionado.")
                    return
                meta = replace(meta, children=selected_children)
            else:
                self._reset_ui_state()
                return

        self._current_meta = meta
        self.inspector.set_metadata(meta)
        self.inspector.setVisible(True)
        self.action_bar.setVisible(True)

    @pyqtSlot(bytes)
    def on_thumbnail_ready(self, data: bytes) -> None:
        if sip.isdeleted(self.inspector): return
        
        if shutil.disk_usage(tempfile.gettempdir()).free < 5_000_000:
            logging.warning("[I/O] Espaço em disco insuficiente para cache de miniaturas.")
            return

        self._analysis_cover_path = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".jpg", prefix="cover_sndstream_").name)
        self._temp_files.append(self._analysis_cover_path)
        
        pixmap = QPixmap()
        if pixmap.loadFromData(data):
            pixmap.toImage().convertToFormat(QImage.Format.Format_RGB32).save(str(self._analysis_cover_path), "JPG", 95)
            if not self.inspector._local_custom_cover_path:
                self.inspector.set_thumbnail(pixmap)

    @pyqtSlot(str)
    def on_analysis_error(self, err_msg: str) -> None:
        QMessageBox.critical(self, "Falha na Análise", err_msg)
        self._reset_ui_state()

    def browse_folder(self) -> None:
        if d := QFileDialog.getExistingDirectory(self, "Localização de Guarda", self.path_input.text()):
            self.path_input.setText(d)

    def open_output_folder(self) -> None:
        path = Path(self.path_input.text())
        if path.exists() and path.is_dir(): QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))

    def queue_download(self) -> None:
        if self._pending_retry_id:
            with self._jobs_lock:
                record = self._jobs.get(self._pending_retry_id)
                config, state = record.config, record.state
            self._spawn_download(config, state, is_retry=False)
            self._reset_ui_state()
            self.url_input.clear()
            return
            
        if not self._current_meta or not self._check_cookie_format(): return
        data = self.inspector.get_config_delta()
        
        is_playlist = getattr(self._current_meta, 'is_playlist', False)
        for entity in (self._current_meta.children if is_playlist else [self._current_meta]):
            job_id = str(uuid.uuid4())
            target_url = UrlResolver.resolve_download_url(entity, getattr(entity, 'original_id', ''))
            source_url = getattr(self.inspector, '_current_source_url', '')
            
            is_audio_centric = getattr(entity, 'is_search_query', False) or any(x in str(target_url).lower() for x in ['ytmsearch', 'ytsearch', 'music.youtube', 'soundcloud']) or any(x in source_url.lower() for x in ['spotify', 'music.youtube', 'soundcloud'])
            if is_audio_centric:
                data['media_type'] = proc.MediaType.AUDIO
                if data['format_container'] in ['mp4', 'mkv', 'webm', 'avi']: data['format_container'] = 'mp3'
            
            final_title = data.get('meta_title', '').strip() if (not is_playlist and data.get('meta_title')) else getattr(entity, 'title', '')
            final_artist = data.get('meta_artist', '').strip() if (not is_playlist and data.get('meta_artist')) else getattr(entity, 'artist', '')
            final_album = data.get('meta_album', '').strip() if (not is_playlist and data.get('meta_album')) else getattr(entity, 'album', '')
            final_genre = data.get('meta_genre', '').strip() if (not is_playlist and data.get('meta_genre')) else getattr(entity, 'genre', '')
            final_date = data.get('meta_date', '').strip() if (not is_playlist and data.get('meta_date')) else (getattr(entity, 'upload_date', '') or '')
            final_desc = data.get('meta_desc', '').strip() if (not is_playlist and data.get('meta_desc')) else (getattr(entity, 'description', '') or '')

            resolved_filename = data.get('custom_filename', '').strip()
            
            if is_playlist or not resolved_filename:
                tmpl = data.get('output_template', '') or "%(title)s - %(artist)s"
                mapping = {
                    'title': final_title,
                    'artist': final_artist,
                    'uploader': final_artist,
                    'album': final_album,
                    'genre': final_genre,
                    'release_year': final_date[:4] if final_date else "",
                    'upload_date': final_date[:4] if final_date else "",
                    'ext': data['format_container']
                }
                def safe_sub(match: re.Match) -> str:
                    key = match.group(1)
                    val = mapping.get(key, f"%({key})s")
                    return re.sub(r'[<>:"/\\|?*]', '', str(val))
                    
                res = re.sub(r'%\(([^)]+)\)s', safe_sub, tmpl)
                res = re.sub(r'%\([^)]+\)s', '', res) # Remove variáveis não resolvidas
                resolved_filename = re.sub(r'\s+', ' ', res).strip(' -_')

            ext = data['format_container']
            if resolved_filename.lower().endswith(f".{ext.lower()}"):
                resolved_filename = resolved_filename[:-(len(ext) + 1)]
                
            if not resolved_filename:
                resolved_filename = "output_stream"
                
            config = proc.DownloadJobConfig(
                job_id=job_id, url=target_url, output_path=Path(self.path_input.text()),
                media_type=data['media_type'], format_container=data['format_container'],
                audio_codec=data['audio_codec'], video_codec=data['video_codec'],
                quality_preset=data['quality_preset'], audio_sample_rate=data['audio_sample_rate'],
                audio_bitrate=str(data['audio_bitrate']), audio_bit_depth=data['audio_bit_depth'],
                output_template=data['output_template'], ffmpeg_path=data['ffmpeg_path'],
                custom_flags=data['custom_flags'], meta_title=final_title,
                meta_artist=final_artist, meta_album=final_album,
                meta_genre=final_genre, meta_date=final_date, meta_desc=final_desc,
                embed_metadata=data['embed_meta'], embed_thumbnail=data['embed_thumb'],
                embed_subs=data['embed_subs'], normalize_audio=data['norm_audio'],
                use_browser_cookies=data['use_cookies'], spotify_thumb_url=getattr(entity, 'thumbnail_url', None)
            )
            
            state = proc.DownloadJobState(
                custom_filename=resolved_filename,
                custom_cover_path=data['local_custom_cover'] or (str(self._analysis_cover_path) if self._analysis_cover_path else None)
            )
            
            with self._jobs_lock:
                self._jobs[job_id] = JobRecord(config=config, state=state)
            self._spawn_download(config, state, False)
            
        self._reset_ui_state()
        self.url_input.clear()

    def retry_job(self, job_id: str) -> None:
        with self._jobs_lock:
            record = self._jobs.get(job_id)
            if not record: return
            config = record.config
            state = copy.deepcopy(record.state)
            
        self._remove_from_queue(job_id)
        
        new_id = str(uuid.uuid4())
        new_config = replace(config, job_id=new_id)
        
        with self._jobs_lock:
            self._jobs[new_id] = JobRecord(config=new_config, state=state)
            
        self.inspector.load_from_config(new_config, state)
        self._current_meta = self.inspector._current_meta
        self.inspector.setVisible(True)
        self.action_bar.setVisible(True)
        self.btn_queue.setText("Confirmar Retentativa")
        self._pending_retry_id = new_id

    def _spawn_download(self, config: proc.DownloadJobConfig, state: proc.DownloadJobState, is_retry: bool) -> None:
        runnable = proc.DownloadWorker(config)
        runnable.state = state
        runnable.signals.progress.connect(self.update_progress)
        runnable.signals.status.connect(self.update_status)
        runnable.signals.finished.connect(lambda: self.on_job_finished(config.job_id))
        runnable.signals.error.connect(lambda err: self.on_job_error(config.job_id, err))
        
        with self._jobs_lock:
            if config.job_id in self._jobs:
                self._jobs[config.job_id].runnable = runnable
            
        if not is_retry:
            row = self.table.rowCount()
            self.table.insertRow(row)
            self._row_index[config.job_id] = row
            
            display = f"{state.custom_filename}.{config.format_container}"
            self.table.setItem(row, 0, QTableWidgetItem(display))
            self.table.item(row, 0).setData(Qt.ItemDataRole.UserRole, config.job_id)
            
            fmt_display = config.format_container.upper()
            if config.media_type == proc.MediaType.VIDEO: fmt_display += f" ({config.quality_preset})"
            self.table.setItem(row, 1, QTableWidgetItem(fmt_display))
            self.table.setItem(row, 2, QTableWidgetItem("Na Fila"))
            
            pbar = QProgressBar()
            pbar.setValue(0)
            self.table.setCellWidget(row, 3, pbar)
            
            btn_cancel = QPushButton("Parar")
            btn_cancel.setObjectName("Destructive")
            btn_cancel.clicked.connect(lambda _, jid=config.job_id: self.cancel_job(jid))
            
            container = QWidget()
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.addWidget(btn_cancel)
            self.table.setCellWidget(row, 4, container)
            
        self.thread_pool.start(runnable)

    def get_row_by_id(self, job_id: str) -> int:
        return self._row_index.get(job_id, -1)

    @pyqtSlot(str, float, str)
    def update_progress(self, job_id: str, pct: float, speed: str) -> None:
        if (row := self.get_row_by_id(job_id)) >= 0:
            if isinstance(w := self.table.cellWidget(row, 3), QProgressBar): w.setValue(int(pct))
            if i := self.table.item(row, 2): i.setText(f"▼ {speed}")

    @pyqtSlot(str, str)
    def update_status(self, job_id: str, msg: str) -> None:
        if (row := self.get_row_by_id(job_id)) >= 0:
            if i := self.table.item(row, 2): i.setText(msg)

    def on_job_finished(self, job_id: str) -> None:
        with self._jobs_lock:
            record = self._jobs.get(job_id)
            if not record or record.is_terminal: return
        self._cleanup_job(job_id, "✔ Concluído", QColor("#4caf50"))

    def on_job_error(self, job_id: str, err: str) -> None:
        with self._jobs_lock:
            record = self._jobs.get(job_id)
            if record: record.is_terminal = True
        self._cleanup_job(job_id, "✘ Erro", QColor("#d32f2f"))
        
        if "NetworkBlockedCDNError" in err or "timeout" in err.lower():
            mitigation_html = (
                "<p>A extração colapsou devido a uma restrição de pacotes (Timeout) no nó de Distribuição de Conteúdo (CDN).</p>"
                "<p>Este comportamento é característico de <b>Redes Corporativas</b> ou académicas equipadas com firewalls restritivos (Inspeção Profunda de Pacotes).</p>"
                "<br><b>Estratégias de Mitigação:</b><ul>"
                "<li><b>Alterar Topologia de Rede:</b> Migrar o host para uma rede externa (ex: Hotspot).</li>"
                "<li><b>Tunelamento Criptográfico (VPN):</b> Ofuscar o tráfego encapsulando a conexão.</li>"
                "<li><b>Configurar Proxy HTTP/SOCKS:</b> Ative a flag <code>--proxy</code> e defina um nó de saída seguro.</li></ul>"
            )
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Icon.Warning)
            msg_box.setWindowTitle("Anomalia de Transporte")
            msg_box.setText(mitigation_html)
            msg_box.exec()

    def cancel_job(self, job_id: str) -> None:
        with self._jobs_lock:
            record = self._jobs.get(job_id)
            if record and record.runnable:
                record.runnable.cancel()
                record.is_terminal = True
        if (row := self.get_row_by_id(job_id)) >= 0:
            if i := self.table.item(row, 2):
                i.setText("A Cancelar...")
                i.setForeground(QColor("#ff9800"))

    def _cleanup_job(self, job_id: str, status_text: str, color: QColor) -> None:
        row = self.get_row_by_id(job_id)
        if row >= 0:
            if i := self.table.item(row, 2):
                i.setText(status_text)
                i.setForeground(color)
            
            if "Concluído" in status_text:
                if isinstance(w := self.table.cellWidget(row, 3), QProgressBar): w.setValue(100)
                btn = QPushButton("Abrir Pasta")
                btn.clicked.connect(self.open_output_folder)
            else:
                if isinstance(w := self.table.cellWidget(row, 3), QProgressBar): w.setValue(0)
                btn = QPushButton("Repetir")
                btn.setObjectName("PrimaryAction")
                btn.clicked.connect(lambda _, jid=job_id: self.retry_job(jid))
                
            container = QWidget()
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.addWidget(btn)
            self.table.setCellWidget(row, 4, container)
            
        QTimer.singleShot(2500, lambda: self._jobs.get(job_id) and setattr(self._jobs[job_id], 'runnable', None))

    def closeEvent(self, event: QCloseEvent) -> None:
        self._save_settings()
        
        with self._jobs_lock:
            for record in self._jobs.values():
                if record.runnable: record.runnable.cancel()
                
        if not self.thread_pool.waitForDone(5000):
            logging.warning("[UI] Timeout no encerramento das threads.")
            
        for f in self._temp_files:
            try: f.unlink(missing_ok=True)
            except: pass
            
        root_logger = logging.getLogger()
        if self.qt_log_handler in root_logger.handlers:
            root_logger.removeHandler(self.qt_log_handler)
            self.qt_log_handler.close()
            
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