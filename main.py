import sys
import argparse
import subprocess
import queue
import threading
import json
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QTextEdit, QLabel,
    QComboBox, QFileDialog, QProgressBar
)
from PyQt6.QtCore import QObject, pyqtSignal, QThread

YT_DLP_PATH = Path.home() / "Downloads" / "yt-dlp.exe"
PRESETS_FILE = "presets.json"

LOSSY_FORMATS = {
    "MP3": ["128k", "192k", "256k", "320k"],
    "AAC": ["128k", "192k", "256k"],
    "OPUS": ["96k", "128k", "160k"]
}

OUTPUT_TEMPLATES = {
    "name.ext": "%(title)s.%(ext)s",
    "name - artist.ext": "%(title)s - %(artist)s.%(ext)s",
    "Álbum / Faixa": "%(artist)s/%(album)s/%(track_number)02d - %(title)s.%(ext)s",
    "Ano - Artista - Música": "%(release_year)s - %(artist)s - %(title)s.%(ext)s",
    "Playlist": "%(playlist_title)s/%(playlist_index)02d - %(title)s.%(ext)s"
}

LOSSLESS_FORMATS = ["WAV", "FLAC"]


def build_command(url, out_dir, fmt, bitrate=None, normalize=False, template=None):
    output_template = OUTPUT_TEMPLATES.get(template, "%(title)s.%(ext)s")

    cmd = [
        str(YT_DLP_PATH),
        "-f", "bestaudio",
        "-x",
        "--yes-playlist",
        "-o", str(Path(out_dir) / output_template),
    ]

    if fmt in LOSSY_FORMATS:
        cmd += ["--audio-format", fmt.lower(),
                "--audio-quality", bitrate.replace("k", "")]
    else:
        cmd += ["--audio-format", fmt.lower()]

    if normalize:
        cmd += [
            "--postprocessor-args",
            "ffmpeg:-af loudnorm=I=-16:TP=-1.5:LRA=11"
        ]

    cmd.append(url)
    return cmd

class DownloadWorker(QObject):
    log = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, job_queue):
        super().__init__()
        self.queue = job_queue
        self.running = True

    def run(self):
        while self.running:
            try:
                job = self.queue.get(timeout=1)
            except queue.Empty:
                continue

            self.log.emit(f"Iniciando: {job['url']}\n")
            process = subprocess.Popen(
                job["cmd"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="ignore"
            )

            for line in process.stdout:
                self.log.emit(line)

            self.queue.task_done()

        self.finished.emit()

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("YouTube Audio Downloader Pro")

        self.queue = queue.Queue()

        self.thread = QThread()
        self.worker = DownloadWorker(self.queue)
        self.worker.moveToThread(self.thread)
        self.worker.log.connect(self.append_log)
        self.thread.started.connect(self.worker.run)
        self.thread.start()

        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        self.url = QLineEdit()
        layout.addWidget(QLabel("URL"))
        layout.addWidget(self.url)

        self.format = QComboBox()
        self.format.addItems(LOSSLESS_FORMATS + list(LOSSY_FORMATS.keys()))
        self.format.currentTextChanged.connect(self.update_bitrate)

        self.bitrate = QComboBox()
        layout.addWidget(QLabel("Formato"))
        layout.addWidget(self.format)
        layout.addWidget(QLabel("Bitrate (lossy)"))
        layout.addWidget(self.bitrate)

        self.normalize = QComboBox()
        self.normalize.addItems(["Não", "Sim (EBU R128)"])
        layout.addWidget(QLabel("Normalização"))
        layout.addWidget(self.normalize)

        self.template = QComboBox()
        self.template.addItems(OUTPUT_TEMPLATES.keys())
        layout.addWidget(QLabel("Padrão do nome do arquivo"))
        layout.addWidget(self.template)

        self.output = QLineEdit()
        btn_out = QPushButton("Selecionar pasta")
        btn_out.clicked.connect(self.choose_dir)

        h = QHBoxLayout()
        h.addWidget(self.output)
        h.addWidget(btn_out)
        layout.addLayout(h)

        btn_add = QPushButton("Adicionar à fila")
        btn_add.clicked.connect(self.add_job)
        layout.addWidget(btn_add)

        self.log = QTextEdit()
        layout.addWidget(self.log)

        self.setLayout(layout)
        self.update_bitrate(self.format.currentText())

    def update_bitrate(self, fmt):
        self.bitrate.clear()
        if fmt in LOSSY_FORMATS:
            self.bitrate.addItems(LOSSY_FORMATS[fmt])
            self.bitrate.setEnabled(True)
        else:
            self.bitrate.setEnabled(False)

    def choose_dir(self):
        d = QFileDialog.getExistingDirectory(self)
        if d:
            self.output.setText(d)

    def add_job(self):
        cmd = build_command(
            url=self.url.text(),
            out_dir=self.output.text(),
            fmt=self.format.currentText(),
            bitrate=self.bitrate.currentText(),
            normalize=self.normalize.currentText().startswith("Sim"),
            template=self.template.currentText()
        )
        self.queue.put({"url": self.url.text(), "cmd": cmd})
        self.append_log("Adicionado à fila.\n")

    def append_log(self, text):
        self.log.append(text)

def run_cli(args):
    cmd = build_command(
        args.url,
        args.output,
        args.format,
        args.bitrate,
        args.normalize
    )
    subprocess.run(cmd)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cli", action="store_true")
    parser.add_argument("--url")
    parser.add_argument("--output", default=".")
    parser.add_argument("--format", default="FLAC")
    parser.add_argument("--bitrate")
    parser.add_argument("--normalize", action="store_true")
    args = parser.parse_args()

    if args.cli:
        run_cli(args)
    else:
        app = QApplication(sys.argv)
        w = MainWindow()
        w.resize(800, 600)
        w.show()
        sys.exit(app.exec())

if __name__ == "__main__":
    main()
