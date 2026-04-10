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
from dataclasses import dataclass, replace
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
from PyQt6.QtCore import Qt, QObject, pyqtSignal, QThreadPool, pyqtSlot, QUrl, QRunnable, QTimer, QAbstractTableModel, QModelIndex
from PyQt6.QtGui import QColor, QPixmap, QFont, QTextCursor, QTextCharFormat, QDesktopServices, QPalette, QAction, QCloseEvent, QImage
from PyQt6 import sip

import processamento as proc


PROJECT_DIR = str(Path(__file__).parent.absolute())
if PROJECT_DIR not in os.environ.get("PATH", ""):
    os.environ["PATH"] = f"{PROJECT_DIR};{os.environ.get('PATH', '')}"

T = TypeVar('T')

APP_NAME: Final[str] = "SoundStream Pro"
VERSION: Final[str] = "7.10.2"
DEFAULT_DOWNLOAD_DIR: Final[Path] = Path.home() / "Downloads"
MAX_CONCURRENT_DOWNLOADS: Final[int] = 3

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
        
        lbl_info = QLabel("Nota: A reescrita efetua-se via <b>Stream Copy</b> atómico. O ficheiro original será substituído de imediato e a nomenclatura atualizada a nível do Sistema Operativo.")
        lbl_info.setWordWrap(True)
        lbl_info.setStyleSheet("color: gray; font-size: 11px;")
        layout.addWidget(lbl_info)
        
        btn_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        btn_box.accepted.connect(self._apply_metadata)
        btn_box.rejected.connect(self.reject)
        layout.addWidget(btn_box)

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
        
        startupinfo = None
        if os.name == 'nt':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            
        try:
            subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, startupinfo=startupinfo)
            
            if target_filepath.exists() and target_filepath.absolute() != self.filepath.absolute():
                target_filepath.unlink(missing_ok=True)
                
            shutil.move(str(temp_out), str(target_filepath))
            
            if target_filepath.absolute() != self.filepath.absolute():
                self.filepath.unlink(missing_ok=True)
                
            self.new_filepath = target_filepath
            QMessageBox.information(self, "Transação Concluída", "Mutação de metadados e topologia de nomenclatura consolidada com êxito.")
            self.accept()
            
        except subprocess.CalledProcessError as e:
            if temp_out.exists(): temp_out.unlink()
            QMessageBox.critical(self, "Kernel Panic (FFmpeg)", f"A transcodificação atómica colapsou.\n\nDetalhes do Processo:\n{e.stderr}")
        except Exception as e:
            if temp_out.exists(): temp_out.unlink()
            QMessageBox.critical(self, "Exceção não Tratada de I/O", str(e))

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
            # Domínio: Rede e Transporte
            EngineFlag("--force-ipv4", "Forçar resolução de sockets via IPv4", category="Rede e Transporte"),
            EngineFlag("--force-ipv6", "Forçar resolução de sockets via IPv6", category="Rede e Transporte"),
            EngineFlag("--limit-rate", "Limitar largura de banda (ex: 50K, 2M)", requires_input=True, category="Rede e Transporte"),
            EngineFlag("--proxy", "URI de Proxy HTTP/SOCKS", requires_input=True, category="Rede e Transporte"),
            EngineFlag("--socket-timeout", "Tempo limite de resposta (Time-to-Live) em segundos", requires_input=True, category="Rede e Transporte"),
            
            # Domínio: Evasão e Falsificação
            EngineFlag("--geo-bypass", "Contornar restrições geográficas via cabeçalhos injetados", category="Evasão de Restrições"),
            EngineFlag("--cookies-from-browser", "Extrair matriz de estado (Cookies) do navegador (ex: chrome, firefox)", requires_input=True, category="Evasão de Restrições"),
            EngineFlag("--user-agent", "Falsificação estrita da string de User-Agent", requires_input=True, category="Evasão de Restrições"),
            
            # Domínio: Regulação de Fluxo e Sincronismo
            EngineFlag("--sleep-requests", "Atraso determinístico entre requisições iterativas (segundos)", requires_input=True, category="Regulação de Fluxo"),
            EngineFlag("--sleep-interval", "Atraso limite inferior (randômico) entre transações (segundos)", requires_input=True, category="Regulação de Fluxo"),
            EngineFlag("--max-sleep-interval", "Atraso limite superior (randômico) entre transações (segundos)", requires_input=True, category="Regulação de Fluxo"),
            
            # Domínio: Operações de Sistema de Arquivos
            EngineFlag("--ignore-errors", "Ignorar exceções isoladas e manter topologia contínua", category="Sistema de Arquivos"),
            EngineFlag("--no-warnings", "Suprimir pipeline de avisos no STDERR", category="Sistema de Arquivos"),
            EngineFlag("--restrict-filenames", "Normalizar nomenclatura para o padrão ASCII (suprime espaços e caracteres especiais)", category="Sistema de Arquivos"),
            EngineFlag("--windows-filenames", "Garantir conversão de nomenclatura para conformidade POSIX/Win32", category="Sistema de Arquivos"),
            EngineFlag("--no-overwrites", "Bloquear sobrescrita (Skip) em partições alocadas previamente", category="Sistema de Arquivos"),
            EngineFlag("--continue", "Forçar a retoma explícita de blocos binários não consolidados", category="Sistema de Arquivos"),
            
            # Domínio: Processamento Estrutural em Árvore
            EngineFlag("--match-filter", "Filtro booleano AST (ex: !is_live & url!*=/shorts/)", requires_input=True, category="Processamento Estrutural"),
            EngineFlag("--playlist-reverse", "Inverter a fila do algoritmo de busca (LIFO)", category="Processamento Estrutural"),
            EngineFlag("--break-on-existing", "Interromper Thread ao encontrar nó persistido (Otimiza rotinas de sincronização)", category="Processamento Estrutural"),
            EngineFlag("--max-downloads", "Limite quantitativo absoluto de nós a extrair", requires_input=True, category="Processamento Estrutural"),
            
            # Domínio: Injeção de Metadados
            EngineFlag("--write-subs", "Efetuar I/O de legendas nativas", category="Metadados e Telemetria"),
            EngineFlag("--write-auto-subs", "Sintetizar matriz de legendas automáticas (ASR)", category="Metadados e Telemetria"),
            EngineFlag("--sub-langs", "Vetor ISO de segmentação de idiomas (ex: en,pt)", requires_input=True, category="Metadados e Telemetria"),
            EngineFlag("--embed-chapters", "Injetar matriz estrutural de capítulos via multiplexador (FFmpeg)", category="Metadados e Telemetria"),
            EngineFlag("--write-info-json", "Descarregar manifesto RAW (JSON) de telemetria da entidade", category="Metadados e Telemetria"),
            
            # Domínio: Acesso de Baixo Nível
            EngineFlag("--extractor-args", "Injeção nativa de dependências lógicas ao módulo base (ex: youtube:player_client=android)", requires_input=True, category="Baixo Nível")
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
        if not self._current_flags:
            return
            
        try:
            tokens = shlex.split(self._current_flags)
        except ValueError as e:
            logging.warning(f"[Lexer] Falha ao efetuar parse da sintaxe AST: {e}")
            tokens = self._current_flags.split()

        for i, token in enumerate(tokens):
            if token in self._ui_elements:
                chk, input_field = self._ui_elements[token]
                chk.setChecked(True)
                
                flag = next((f for f in self._flag_registry if f.cli_arg == token), None)
                if flag and flag.requires_input and input_field:
                    if i + 1 < len(tokens) and not tokens[i+1].startswith("--"):
                        input_field.setText(tokens[i+1])

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
            QLineEdit[readOnly="true"] { background-color: UID_DISABLED_BG; color: UID_DISABLED_TXT; border: 1px dashed UID_BORDER; }
        """
        if is_dark:
            return qss.replace("UID_BORDER", "#3d3d3d").replace("UID_PANEL_BG", "#1e1e1e").replace("UID_THUMB_BG", "#000000").replace("UID_THUMB_TEXT", "#aaaaaa").replace("UID_CONSOLE_BG", "#0e0e0e").replace("UID_CONSOLE_TEXT", "#d4d4d4").replace("UID_DISABLED_BG", "#2a2a2a").replace("UID_DISABLED_TXT", "#888888")
        return qss.replace("UID_BORDER", "#cccccc").replace("UID_PANEL_BG", "#ffffff").replace("UID_THUMB_BG", "#eaeaea").replace("UID_THUMB_TEXT", "#666666").replace("UID_CONSOLE_BG", "#ffffff").replace("UID_CONSOLE_TEXT", "#333333").replace("UID_DISABLED_BG", "#f0f0f0").replace("UID_DISABLED_TXT", "#777777")

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
    def __init__(self, entities: List[proc.NormalizedMediaEntity], parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._entities = entities
        self._checked_states = [True] * len(entities)
        self._headers = ["Inc.", "Título", "Artista", "Álbum", "Duração"]

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._entities)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._headers)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None
            
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
        self.beginResetModel()
        self._checked_states = [state] * len(self._entities)
        self.endResetModel()

    def get_selected_entities(self) -> List[proc.NormalizedMediaEntity]:
        return [ent for i, ent in enumerate(self._entities) if self._checked_states[i]]
    
class PlaylistStagingDialog(QDialog):
    def __init__(self, entities: List[proc.NormalizedMediaEntity], parent: Optional[QWidget] = None) -> None:
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

    def get_selected_entities(self) -> List[proc.NormalizedMediaEntity]:
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
        self._current_source_url: str = ""
        self._local_custom_cover_path: str = ""
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
        self.in_filename.setReadOnly(True)
        self.in_filename.setToolTip("Gerado ativamente (Tempo-Real) com base no Template de Saída.")
        
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
        self.btn_import_cookies.setToolTip("Injetar estado de sessão (cookies.txt) no diretório de execução.")
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
        
        default_flags = (
            '--extractor-args "youtube:player_client=android,tv" '
            '--match-filter "!is_live & url!*=/shorts/ & title!~=\'(?i)(official video|music video|videoclipe|clip|visualizer|live|cover|karaoke|instrumental|acústico|acoustic)\'" '
            '--force-ipv4'
        )
        self.in_custom_flags = QLineEdit(default_flags, dev_group)
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
        
        self.in_title.textChanged.connect(self._update_dynamic_filename)
        self.in_artist.textChanged.connect(self._update_dynamic_filename)
        self.in_album.textChanged.connect(self._update_dynamic_filename)
        self.in_genre.textChanged.connect(self._update_dynamic_filename)
        self.in_date.textChanged.connect(self._update_dynamic_filename)
        self.in_output_tmpl.textChanged.connect(self._update_dynamic_filename)
        self.cb_container.currentTextChanged.connect(self._update_dynamic_filename)

    @pyqtSlot()
    def _open_flags_editor(self) -> None:
        if sip.isdeleted(self.in_custom_flags): return
        
        current_state = self.in_custom_flags.text().strip()
        dialog = EngineFlagsDialog(current_state, self)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            new_flags = dialog.compile_flags()
            self.in_custom_flags.setText(new_flags)

    def load_from_config(self, config: Any) -> None:
        if getattr(config, 'media_type', None) == proc.MediaType.VIDEO:
            self.rb_video.setChecked(True)
        else:
            self.rb_audio.setChecked(True)
            
        self._update_ui_mode()

        container = getattr(config, 'format_container', '')
        if container:
            idx = self.cb_container.findText(container, Qt.MatchFlag.MatchExactly)
            if idx >= 0: self.cb_container.setCurrentIndex(idx)

        if hasattr(config, 'video_codec'):
            idx = self.cb_vcodec.findText(config.video_codec, Qt.MatchFlag.MatchContains | Qt.MatchFlag.MatchCaseSensitive)
            if idx >= 0: self.cb_vcodec.setCurrentIndex(idx)

        if hasattr(config, 'audio_codec'):
            idx = self.cb_acodec.findText(config.audio_codec, Qt.MatchFlag.MatchContains | Qt.MatchFlag.MatchCaseSensitive)
            if idx >= 0: self.cb_acodec.setCurrentIndex(idx)

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
            if not tmpl.endswith(".%(ext)s"):
                tmpl += ".%(ext)s"
            self.in_output_tmpl.setText(tmpl)

        if hasattr(config, 'ffmpeg_path'):
            self.in_ffmpeg_path.setText(config.ffmpeg_path)

        if hasattr(config, 'custom_flags'):
            if hasattr(self, 'in_custom_flags') and not sip.isdeleted(self.in_custom_flags):
                self.in_custom_flags.setText(config.custom_flags)

        class MockEntity:
            def __init__(self, cfg):
                self.is_playlist = False
                self.children = []
                self.original_id = getattr(cfg, 'url', '')
                self.duration = 0.0
                self.filesize = 0
                self.is_search_query = False
                self.width = None
                self.height = None
                self.fps = None
                self.channel = getattr(cfg, 'meta_artist', 'Desconhecido')
                
                self.title = getattr(cfg, 'meta_title', '')
                self.artist = getattr(cfg, 'meta_artist', '')
                self.album = getattr(cfg, 'meta_album', '')
                self.genre = getattr(cfg, 'meta_genre', '')
                self.upload_date = getattr(cfg, 'meta_date', '')
                self.description = getattr(cfg, 'meta_desc', '')
                self.thumbnail_url = getattr(cfg, 'spotify_thumb_url', None)

        self._current_meta = MockEntity(config)
        self._current_source_url = getattr(config, 'url', '')

        if hasattr(config, 'custom_cover_path') and config.custom_cover_path:
            path = Path(config.custom_cover_path)
            if path.exists():
                self._local_custom_cover_path = str(path)
                self.set_thumbnail(QPixmap(str(path)))

        self._update_dynamic_filename()

    @pyqtSlot()
    def _update_dynamic_filename(self) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.in_output_tmpl) or sip.isdeleted(self.in_filename): 
            return
            
        tmpl = self.in_output_tmpl.text().strip()
        if not tmpl:
            self.in_filename.setText("")
            self.in_output_tmpl.setStyleSheet("border: 1px solid #d32f2f;")
            self.in_output_tmpl.setToolTip("Erro de Consistência: O Template não pode ser nulo.")
            return
            
        self.in_output_tmpl.setStyleSheet("")
        
        is_batch = not self.tabs.isTabEnabled(1)
        
        title = self.in_title.text().strip() or ("Título" if not is_batch else "Variáveis em Lote")
        artist = self.in_artist.text().strip() or ("Artista" if not is_batch else "Vários Artistas")
        album = self.in_album.text().strip() or ("Álbum" if not is_batch else "Vários Álbuns")
        genre = self.in_genre.text().strip() or "Género"
        date_full = self.in_date.text().strip()
        year = date_full[:4] if date_full else "Ano"
        ext = self.cb_container.currentText() if not sip.isdeleted(self.cb_container) else "ext"
        
        mapping = {
            'title': title,
            'artist': artist,
            'uploader': artist,
            'album': album,
            'genre': genre,
            'release_year': year,
            'upload_date': date_full or "Data",
            'ext': ext,
            'playlist': "Playlist",
            'playlist_index': "01"
        }
        
        def safe_sub(match: re.Match) -> str:
            key = match.group(1)
            val = mapping.get(key, f"%({key})s")
            return re.sub(r'[<>:"/\\|?*]', '', str(val))
            
        try:
            res = re.sub(r'%\(([^)]+)\)s', safe_sub, tmpl)
            res = re.sub(r'\s+', ' ', res).strip(' -_')
            self.in_filename.setText(res)
            
            if "%(" in res:
                self.in_output_tmpl.setStyleSheet("border: 1px solid #ff9800;")
                self.in_output_tmpl.setToolTip("Aviso Léxico: Foram detetadas variáveis órfãs/não preenchidas.")
            else:
                self.in_output_tmpl.setToolTip("Sintaxe AST Válida")
                
        except Exception as e:
            self.in_filename.setText("Falha na Resolução Lexical")
            self.in_output_tmpl.setStyleSheet("border: 1px solid #d32f2f;")
            self.in_output_tmpl.setToolTip(f"Erro: {str(e)}")

    def _browse_custom_cover(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Selecionar Capa Personalizada", "", "Imagens (*.jpg *.jpeg *.png)")
        if path:
            self._local_custom_cover_path = path
            pixmap = QPixmap(path)
            self.set_thumbnail(pixmap)

    def _import_cookies(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Selecionar Matriz de Cookies", "", "Ficheiros de Texto (*.txt);;Todos os Ficheiros (*)")
        if not path:
            return

        try:
            source_path = Path(path)
            with open(source_path, "r", encoding="utf-8", errors="ignore") as f:
                header = f.read(120)
                if "# Netscape HTTP Cookie File" not in header:
                    QMessageBox.warning(self, "Anomalia Heurística", "A assinatura léxica do ficheiro não corresponde à RFC 'Netscape HTTP Cookie File'. A importação foi abortada para garantir a segurança da Thread de Extração.")
                    return

            target_path = Path.cwd() / "cookies.txt"
            
            success = False
            for attempt in range(5):
                try:
                    temp_path = target_path.with_suffix(f".tmp{attempt}")
                    shutil.copy2(source_path, temp_path)
                    os.replace(temp_path, target_path)
                    success = True
                    break
                except PermissionError:
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    time.sleep(0.5 * (1.5 ** attempt))
                except Exception as e:
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                    raise e

            if not success:
                QMessageBox.warning(
                    self, 
                    "Violação de Mutex (WinError 32)", 
                    "O descritor do ficheiro 'cookies.txt' encontra-se bloqueado pelo motor de análise em background.\n\n"
                    "Aguarde a libertação do socket ou cancele as transferências ativas antes de injetar uma nova sessão."
                )
                return

            self.chk_cookies.setChecked(True)
            QMessageBox.information(self, "Estado Injetado", f"O descritor de estado foi transacionado de forma atómica para o workspace de execução:\n{target_path.absolute()}")
        except Exception as e:
            QMessageBox.critical(self, "Falha de I/O", f"Exceção não tratada ao copiar a matriz binária:\n{e}")

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
        self._local_custom_cover_path = str(cache_dir / f"cover_sndstream_custom_{uuid.uuid4().hex[:8]}.jpg")

        safe_image = image.convertToFormat(QImage.Format.Format_RGB32)
        safe_image.save(self._local_custom_cover_path, "JPG", 95)
        
        logging.info(f"[I/O] Arte primária fixada e convertida (RGB32). Sobrescrita bloqueada: {self._local_custom_cover_path}")

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
        
        if not sip.isdeleted(self.in_title): self.in_title.setText(meta.title)
        if not sip.isdeleted(self.in_artist): self.in_artist.setText(meta.artist)
        if not sip.isdeleted(self.in_album): self.in_album.setText(meta.album)
        
        if not sip.isdeleted(self.in_date): 
            self.in_date.setText(meta.upload_date[:4] if meta.upload_date else "")
        if not sip.isdeleted(self.in_desc): 
            self.in_desc.setPlainText(meta.description if meta.description else "")
            
        if not sip.isdeleted(self.in_genre): self.in_genre.clear()
        
        entities = getattr(meta, 'children', []) if getattr(meta, 'is_playlist', False) else []
        is_batch_operation = len(entities) > 1
        
        self.tabs.setTabEnabled(1, not is_batch_operation)
        if is_batch_operation:
            self.tabs.setTabToolTip(1, "Metadados globais bloqueados para transações em lote (Playlists).")
            self.in_title.clear()
            self.in_artist.clear()
            self.in_album.clear()
        else:
            self.tabs.setTabToolTip(1, "")
        
        is_audio_restricted = False
        orig_id = getattr(meta, 'original_id', '')
        if isinstance(orig_id, str) and ('ytmsearch' in orig_id or 'ytsearch' in orig_id or 'music.youtube' in orig_id or 'soundcloud' in orig_id):
            is_audio_restricted = True
            
        if getattr(meta, 'children', None):
            for c in meta.children:
                c_id = getattr(c, 'original_id', '')
                if isinstance(c_id, str) and ('ytmsearch' in c_id or 'ytsearch' in c_id or 'music.youtube' in c_id or 'soundcloud' in c_id):
                    is_audio_restricted = True
                    break

        if is_audio_restricted:
            self.rb_audio.setChecked(True)
            self.rb_video.setEnabled(False)
            self.rb_video.setToolTip("Vídeo bloqueado: A topologia de origem restringe a extração ao formato de áudio.")
        else:
            self.rb_video.setEnabled(True)
            self.rb_video.setToolTip("")

        is_playlist_mode = getattr(meta, 'is_playlist', False)
        self.btn_custom_cover.setEnabled(not is_playlist_mode)
        if is_playlist_mode:
            self.btn_custom_cover.setToolTip("A injeção de matrizes gráficas unificadas é inválida para estruturas em árvore (Playlists).")
        else:
            self.btn_custom_cover.setToolTip("")
            
        self._recalc_estimate()
        self._update_dynamic_filename()

    def set_thumbnail(self, pixmap: QPixmap) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.thumb_lbl): return
        self.thumb_lbl.setPixmap(pixmap)

    def clear_thumbnail(self) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.thumb_lbl): return
        self.thumb_lbl.clear()
        self.thumb_lbl.setText("Sem Pré-visualização")
        self._local_custom_cover_path = ""

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
        self._update_dynamic_filename()

    def _recalc_estimate(self) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.stats_lbl) or not self._current_meta: return
        
        meta = self._current_meta
        is_playlist = getattr(meta, 'is_playlist', False)
        children = getattr(meta, 'children', []) or []
        
        orig_id = getattr(meta, 'original_id', '')
        source_url = getattr(self, '_current_source_url', '').lower()
        
        is_spotify_ytm = getattr(meta, 'is_search_query', False) or "spotify" in orig_id or "music.youtube" in orig_id or "spotify" in source_url or "music.youtube" in source_url
        is_soundcloud = "soundcloud.com" in orig_id or "soundcloud" in source_url
        is_video_type = self.rb_video.isChecked()

        total_duration = 0.0
        total_filesize = 0

        if is_playlist:
            total_duration = sum(float(getattr(c, 'duration', 0) or 0) for c in children)
            total_filesize = sum(int(getattr(c, 'filesize', 0) or 0) for c in children)
        else:
            total_duration = float(getattr(meta, 'duration', 0) or 0)
            total_filesize = int(getattr(meta, 'filesize', 0) or 0)

        if total_filesize <= 0 and total_duration > 0:
            bitrate_kbps = 128
            if is_video_type:
                q_text = self.cb_quality.currentText().lower()
                if "4k" in q_text or "2160" in q_text: bitrate_kbps = 15000
                elif "1440" in q_text: bitrate_kbps = 8000
                elif "1080" in q_text: bitrate_kbps = 5000
                elif "720" in q_text: bitrate_kbps = 2500
                elif "480" in q_text: bitrate_kbps = 1000
                else: bitrate_kbps = 5000 
            else:
                fmt = self.cb_container.currentText().lower()
                if fmt in ['flac', 'wav']:
                    bitrate_kbps = 900 if fmt == 'flac' else 1411
                else:
                    try:
                        bitrate_kbps = int(self.cb_abitrate.currentData() or 192)
                    except ValueError:
                        bitrate_kbps = 192
            
            total_filesize = int((bitrate_kbps * 1000 / 8) * total_duration)

        def format_size(size_bytes: int) -> str:
            if size_bytes <= 0: return "N/A"
            mb = size_bytes / (1024 * 1024)
            if mb >= 1024:
                return f"{mb / 1024:.2f} GB"
            return f"{mb:.2f} MB"

        def format_duration(seconds: float) -> str:
            s = int(seconds)
            if s <= 0: return "N/A"
            mins, secs = divmod(s, 60)
            hrs, mins = divmod(mins, 60)
            if hrs > 0: return f"{hrs:02d}:{mins:02d}:{secs:02d}"
            return f"{mins:02d}:{secs:02d}"

        html_parts = []

        if is_playlist:
            html_parts.append(f"<b>Duração Total:</b> {format_duration(total_duration)}")
            html_parts.append(f"<b>Tamanho Projetado:</b> {format_size(total_filesize)}")
            html_parts.append(f"<b>Total de Nós (Faixas):</b> {len(children)}")
            html_parts.append("<b>Topologia:</b> Conjunto de Dados Escalar (Playlist/Álbum)")
        else:
            html_parts.append(f"<b>Duração:</b> {format_duration(total_duration)}")
            html_parts.append(f"<b>Tamanho Projetado:</b> {format_size(total_filesize)}")
            
            if is_spotify_ytm:
                album = getattr(meta, 'album', '') or 'Desconhecido'
                date = getattr(meta, 'upload_date', '') or 'N/A'
                html_parts.append(f"<b>Álbum:</b> {album}")
                html_parts.append(f"<b>Lançamento:</b> {date}")
                html_parts.append("<b>Topologia:</b> Fonograma Master (DRM/Áudio)")
            elif is_soundcloud:
                canal = getattr(meta, 'channel', '') or getattr(meta, 'artist', 'Desconhecido')
                html_parts.append(f"<b>Uploader (SC):</b> {canal}")
                html_parts.append("<b>Topologia:</b> Áudio de Plataforma Fechada")
            elif getattr(meta, 'width', None) is not None:
                res = f"{meta.width}x{meta.height} @ {getattr(meta, 'fps', 'N/A')}fps"
                canal = getattr(meta, 'channel', '') or 'Desconhecido'
                html_parts.append(f"<b>Matriz de Vídeo:</b> {res}")
                html_parts.append(f"<b>Canal (YT):</b> {canal}")
            else:
                canal = getattr(meta, 'channel', '') or 'Desconhecido'
                html_parts.append(f"<b>Canal:</b> {canal}")

        self.stats_lbl.setText("<br>".join(html_parts))

    def get_config_delta(self) -> Dict[str, Any]:
        if sip.isdeleted(self) or sip.isdeleted(self.tabs):
            return {}

        asr_data = self.cb_asr.currentData() if not sip.isdeleted(self.cb_asr) else "auto"
        audio_sample_rate = 0 if asr_data == "auto" else int(asr_data)
        
        bitdepth_text = self.cb_bitdepth.currentText() if not sip.isdeleted(self.cb_bitdepth) else "Auto"
        audio_bit_depth = bitdepth_text.split('-')[0] if "bit" in bitdepth_text else "auto"

        raw_tmpl = self.in_output_tmpl.text().strip() if not sip.isdeleted(self.in_output_tmpl) else ""
        backend_tmpl = raw_tmpl
        if backend_tmpl.endswith(".%(ext)s"):
            backend_tmpl = backend_tmpl[:-8]
        elif backend_tmpl.endswith(".%(ext)"):
            backend_tmpl = backend_tmpl[:-7]

        is_batch = not self.tabs.isTabEnabled(1)
        preview_name = self.in_filename.text().strip()
        ext = self.cb_container.currentText() if not sip.isdeleted(self.cb_container) else ""
        
        if preview_name.endswith(f".{ext}"):
            preview_name = preview_name[:-(len(ext)+1)]
            
        final_custom_filename = "" if is_batch else preview_name

        return {
            'media_type': proc.MediaType.VIDEO if self.rb_video.isChecked() else proc.MediaType.AUDIO,
            'format_container': self.cb_container.currentText() if not sip.isdeleted(self.cb_container) else "mp3",
            'video_codec': self.cb_vcodec.currentText().lower() if not sip.isdeleted(self.cb_vcodec) else "best",
            'audio_codec': self.cb_acodec.currentText().lower() if not sip.isdeleted(self.cb_acodec) else "best",
            'quality_preset': self.cb_quality.currentText() if not sip.isdeleted(self.cb_quality) else "Best",
            'audio_bitrate': self.cb_abitrate.currentData() if not sip.isdeleted(self.cb_abitrate) and self.cb_abitrate.isEnabled() else "0",
            'audio_sample_rate': audio_sample_rate,
            'audio_bit_depth': audio_bit_depth, 
            'custom_filename': final_custom_filename,
            
            'output_template': backend_tmpl,
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
            'local_custom_cover': getattr(self, '_local_custom_cover_path', "")
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
        self.job_configs: Dict[str, proc.DownloadJobConfig] = {}
        self._terminal_jobs: Set[str] = set()
        
        self._current_meta: Optional[proc.NormalizedMediaEntity] = None

        self._analysis_cover_path: Optional[Path] = None

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
            header_view.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        
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
                status_item = self.table.item(row, 2)
                
                if status_item and "Concluído" in status_item.text():
                    action_edit_meta = QAction("Editar Metadados Localmente", self)
                    action_edit_meta.triggered.connect(lambda _, jid=job_id: self._edit_local_metadata(jid))
                    menu.addAction(action_edit_meta)
                
                action_remove = QAction("Remover da Fila", self)
                action_remove.triggered.connect(lambda _, jid=job_id: self._remove_from_queue(jid))
                menu.addAction(action_remove)
                
                if job_id in self.active_runnables:
                    action_cancel = QAction("Cancelar Transferência", self)
                    action_cancel.triggered.connect(lambda _, jid=job_id: self.cancel_job(jid))
                    menu.addAction(action_cancel)
                else:
                    if status_item and ("Erro" in status_item.text() or "Cancelado" in status_item.text()):
                        action_retry = QAction("Tentar Novamente", self)
                        action_retry.triggered.connect(lambda _, jid=job_id: self.retry_job(jid))
                        menu.addAction(action_retry)
                    
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

    def _edit_local_metadata(self, job_id: str) -> None:
        config = self.job_configs.get(job_id)
        if not config: return
        
        resolved_path_str = getattr(config, 'resolved_output_path', None)
        if resolved_path_str and Path(resolved_path_str).exists():
            filepath = Path(resolved_path_str)
        else:
            safe_name = re.sub(r'[\\/*?:"<>|]', '_', config.custom_filename)
            filepath = Path(config.output_path) / f"{safe_name}.{config.format_container}"
            if not filepath.exists():
                candidates = list(Path(config.output_path).glob(f"*.{config.format_container}"))
                if candidates:
                    filepath = max(candidates, key=lambda p: p.stat().st_mtime)
        
        if not filepath or not filepath.exists():
            QMessageBox.warning(self, "Aviso", f"O ficheiro físico não se encontra na topologia indexada:\n{filepath}")
            return
            
        initial_data = {
            'meta_title': config.meta_title,
            'meta_artist': config.meta_artist,
            'meta_album': config.meta_album,
            'meta_genre': config.meta_genre,
            'meta_date': config.meta_date,
            'meta_desc': config.meta_desc,
        }
            
        dialog = LocalMetadataEditorDialog(str(filepath), initial_data, self)
        if dialog.exec() == QDialog.DialogCode.Accepted and dialog.new_filepath:
            object.__setattr__(config, "custom_filename", dialog.new_filepath.stem)
            object.__setattr__(config, "resolved_output_path", str(dialog.new_filepath))
            
            object.__setattr__(config, "meta_title", dialog.in_title.text())
            object.__setattr__(config, "meta_artist", dialog.in_artist.text())
            object.__setattr__(config, "meta_album", dialog.in_album.text())
            object.__setattr__(config, "meta_genre", dialog.in_genre.text())
            object.__setattr__(config, "meta_date", dialog.in_date.text())
            object.__setattr__(config, "meta_desc", dialog.in_desc.toPlainText())
            
            row = self.get_row_by_id(job_id)
            if row >= 0:
                item = self.table.item(row, 0)
                if item is not None:
                    item.setText(dialog.new_filepath.name)
                    item.setToolTip(dialog.in_title.text())

    def _remove_from_queue(self, job_id: str) -> None:
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            self._terminal_jobs.add(job_id)
            
        if job_id in self.job_configs:
            del self.job_configs[job_id]
            
        row = self.get_row_by_id(job_id)
        if row >= 0:
            self.table.removeRow(row)

    def _clear_finished_jobs(self) -> None:
        for row in range(self.table.rowCount() - 1, -1, -1):
            status_item = self.table.item(row, 2)
            if status_item is not None and status_item.text() in ["✔ Concluído", "✘ Erro", "Cancelado"]:
                job_id = self.table.item(row, 0).data(Qt.ItemDataRole.UserRole)
                if job_id in self.job_configs:
                    del self.job_configs[job_id]
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
    def on_analysis_success(self, meta: proc.NormalizedMediaEntity) -> None:
        if sip.isdeleted(self) or sip.isdeleted(self.inspector): return

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
        
        if not getattr(self.inspector, '_local_custom_cover_path', None):
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
        
        is_playlist_mode = getattr(self._current_meta, 'is_playlist', False)
        entities_to_process: List[proc.NormalizedMediaEntity] = (
            self._current_meta.children if is_playlist_mode and self._current_meta.children
            else [self._current_meta]
        )

        for entity in entities_to_process:
            job_id = str(uuid.uuid4())
            target_url = str(getattr(entity, 'original_id', ''))
            
            source_url = getattr(self.inspector, '_current_source_url', '')
            is_audio_centric = getattr(entity, 'is_search_query', False) or "music.youtube" in target_url or "spotify" in target_url or "soundcloud" in target_url or "music.youtube" in source_url or "spotify" in source_url or "soundcloud" in source_url
            
            current_media_type = data['media_type']
            current_format = data['format_container']
            
            if is_audio_centric:
                current_media_type = proc.MediaType.AUDIO
                if current_format in ['mp4', 'mkv', 'webm', 'avi']:
                    current_format = 'mp3' 
            
            if getattr(entity, 'is_search_query', False) or target_url.startswith("ytmsearch") or target_url.startswith("ytsearch"):
                query = target_url.split(":", 1)[-1] if ":" in target_url else target_url
                target_url = f'ytsearch5:{query.strip()} "Provided to YouTube"'
                
            elif not target_url.startswith("http") and not target_url.startswith("ytsearch"):
                target_url = f"https://www.youtube.com/watch?v={target_url}"

            final_title = data.get('meta_title', '').strip() if (not is_playlist_mode and data.get('meta_title')) else (entity.title or "")
            final_artist = data.get('meta_artist', '').strip() if (not is_playlist_mode and data.get('meta_artist')) else (entity.artist or "")
            final_album = data.get('meta_album', '').strip() if (not is_playlist_mode and data.get('meta_album')) else (entity.album or "")
            final_genre = data.get('meta_genre', '').strip() if (not is_playlist_mode and data.get('meta_genre')) else getattr(entity, 'genre', '')
            final_date = data.get('meta_date', '').strip() if (not is_playlist_mode and data.get('meta_date')) else (getattr(entity, 'upload_date', '') or '')
            final_desc = data.get('meta_desc', '').strip() if (not is_playlist_mode and data.get('meta_desc')) else (getattr(entity, 'description', '') or '')

            resolved_filename = data.get('custom_filename', '').strip()
            if is_playlist_mode or not resolved_filename:
                tmpl = data.get('output_template', '%(title)s - %(artist)s')
                if not tmpl:
                    tmpl = "%(title)s - %(artist)s"
                    
                mapping = {
                    'title': final_title,
                    'artist': final_artist,
                    'uploader': final_artist,
                    'album': final_album,
                    'genre': final_genre,
                    'release_year': final_date[:4] if final_date else "",
                    'upload_date': final_date[:4] if final_date else ""
                }
                
                def safe_sub(match: re.Match) -> str:
                    key = match.group(1)
                    val = mapping.get(key, f"%({key})s")
                    return re.sub(r'[<>:"/\\|?*]', '', str(val))
                    
                res = re.sub(r'%\(([^)]+)\)s', safe_sub, tmpl)
                res = re.sub(r'%\([^)]+\)s', '', res)
                res = re.sub(r'\s+', ' ', res).strip(' -_')
                resolved_filename = res or "output_stream"

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
                meta_title=final_title,
                meta_artist=final_artist,
                meta_album=final_album,
                meta_genre=final_genre,
                meta_date=final_date,
                meta_desc=final_desc,
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

            user_local_cover = data.get('local_custom_cover', '')
            analysis_cover = self._analysis_cover_path
            final_cover = user_local_cover if user_local_cover else analysis_cover
            object.__setattr__(config, "custom_cover_path", str(final_cover) if final_cover else "")
            
            self.job_configs[job_id] = config
            self._spawn_download(config, is_retry=False)
        
        if not sip.isdeleted(self.inspector):
            self.inspector.setVisible(False)
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(False)
        self.url_input.clear()

    def retry_job(self, job_id: str) -> None:
        if job_id in self._terminal_jobs:
            self._terminal_jobs.remove(job_id)
            
        config = self.job_configs.get(job_id)
        if not config: return
        
        self.url_input.blockSignals(True)
        self.url_input.setText(config.url)
        self.url_input.blockSignals(False)
        
        if not sip.isdeleted(self.inspector):
            self.inspector.load_from_config(config)
            self._current_meta = self.inspector._current_meta 
            self.inspector.setVisible(True)
            
        if not sip.isdeleted(self.action_bar):
            self.action_bar.setVisible(True)
            
        self._remove_from_queue(job_id)
        

    def _spawn_download(self, config: proc.DownloadJobConfig, is_retry: bool = False) -> None:
        runnable = proc.DownloadWorker(config)
        runnable.signals.progress.connect(self.update_progress)
        runnable.signals.status.connect(self.update_status)
        runnable.signals.finished.connect(lambda: self.on_job_finished(config.job_id))
        runnable.signals.error.connect(lambda err: self.on_job_error(config.job_id, err))
        self.active_runnables[config.job_id] = runnable
        
        if is_retry:
            row = self.get_row_by_id(config.job_id)
            if row >= 0:
                self._reset_row_for_retry(row, config.job_id)
        else:
            self.add_table_row(config)
            
        self.thread_pool.start(runnable)

    def _reset_row_for_retry(self, row: int, job_id: str) -> None:
        btn_cancel = QPushButton("Parar")
        btn_cancel.setObjectName("Destructive")
        btn_cancel.clicked.connect(lambda _, jid=job_id: self.cancel_job(jid))
        
        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(btn_cancel)
        container = QWidget()
        container.setLayout(layout)
        
        self.table.setCellWidget(row, 4, container)

    def add_table_row(self, config: proc.DownloadJobConfig) -> None:
        row = self.table.rowCount()
        self.table.insertRow(row)
        
        display_name = config.custom_filename if config.custom_filename else "Em processamento"
        if display_name and config.format_container:
            display_name = f"{display_name}.{config.format_container}"
            
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
        if job_id in self._terminal_jobs: return
        self._cleanup_job(job_id, "✔ Concluído", QColor("#4caf50"))

    def on_job_error(self, job_id: str, err: str) -> None:
        if sip.isdeleted(self): return
        
        self._terminal_jobs.add(job_id)
        self._cleanup_job(job_id, "✘ Erro", QColor("#d32f2f"))

        err_lower = err.lower()
        is_cdn_block = "googlevideo.com" in err_lower and ("timed out" in err_lower or "timeout" in err_lower)
        is_custom_exc = "NetworkBlockedCDNError" in err
        
        if is_cdn_block or is_custom_exc:
            mitigation_html = (
                "<p>A extração colapsou devido a uma restrição de pacotes (Timeout) no nó de Distribuição de Conteúdo (CDN).</p>"
                "<p>Este comportamento é característico de <b>Redes Corporativas</b> ou académicas equipadas com firewalls restritivos (Inspeção Profunda de Pacotes).</p>"
                "<br>"
                "<b>Estratégias de Mitigação:</b>"
                "<ul>"
                "<li><b>Alterar Topologia de Rede:</b> Migrar o host para uma rede externa (ex: Hotspot 4G/5G).</li>"
                "<li><b>Tunelamento Criptográfico (VPN):</b> Ofuscar o tráfego da camada de transporte encapsulando a conexão.</li>"
                "<li><b>Configurar Proxy HTTP/SOCKS:</b> No painel de Configuração Avançada, em <i>Parâmetros do Motor</i>, ative a flag <code>--proxy</code> e defina um nó de saída seguro.</li>"
                "</ul>"
            )
            
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Icon.Warning)
            msg_box.setWindowTitle("Anomalia de Transporte: Bloqueio de Infraestrutura")
            msg_box.setText(mitigation_html)
            msg_box.exec()

    def cancel_job(self, job_id: str) -> None:
        if job_id in self.active_runnables:
            self.active_runnables[job_id].cancel()
            self._terminal_jobs.add(job_id)
            row = self.get_row_by_id(job_id)
            if row >= 0:
                item = self.table.item(row, 2)
                if item is not None:
                    item.setText("A Cancelar...")
                    item.setForeground(QColor("#ff9800"))

    def _cleanup_job(self, job_id: str, status_text: str, color: QColor) -> None:
        row = self.get_row_by_id(job_id)
        if row >= 0:
            item = self.table.item(row, 2)
            if item is not None:
                item.setText(status_text)
                item.setForeground(color)
            
            config = self.job_configs.get(job_id)
            if config and getattr(config, 'custom_filename', None):
                title_item = self.table.item(row, 0)
                if title_item is not None:
                    title_item.setText(f"{config.custom_filename}.{config.format_container}")

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
                
            elif "Cancelado" in status_text or "Erro" in status_text:
                widget = self.table.cellWidget(row, 3)
                if isinstance(widget, QProgressBar):
                    widget.setValue(0)
                
                btn_retry = QPushButton("Repetir") 
                btn_retry.setMinimumWidth(120)
                btn_retry.setObjectName("PrimaryAction")
                btn_retry.clicked.connect(lambda _, jid=job_id: self.retry_job(jid))
                
                layout = QHBoxLayout()
                layout.setContentsMargins(0, 0, 0, 0)
                layout.addWidget(btn_retry)
                container = QWidget()
                container.setLayout(layout)
                self.table.setCellWidget(row, 4, container)
                
        def _safe_remove() -> None:
            if job_id in self.active_runnables: 
                del self.active_runnables[job_id]
                
        QTimer.singleShot(2500, _safe_remove)

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