from __future__ import annotations

import sys
import threading
from datetime import datetime
from pathlib import Path

from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QCloseEvent
from PyQt5.QtWidgets import (
    QApplication,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from putaway_worker import run_job


class PutawayThread(QThread):
    log_message = pyqtSignal(str)
    finished = pyqtSignal(bool, str)

    def __init__(self, csv_path: Path, output_dir: Path, stop_event: threading.Event) -> None:
        super().__init__()
        self._csv_path = csv_path
        self._output_dir = output_dir
        self._stop_event = stop_event

    def run(self) -> None:
        def emit_log(text: str) -> None:
            self.log_message.emit(text)

        try:
            ok, msg = run_job(self._csv_path, self._output_dir, self._stop_event, emit_log)
            self.finished.emit(ok, msg)
        except Exception as e:
            self.log_message.emit(f"处理异常: {e}")
            self.finished.emit(False, str(e))


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("自动上架工具")
        self.setMinimumSize(720, 520)
        self._csv_path: Path | None = None
        self._is_running = False
        self._stop_event: threading.Event | None = None
        self._worker: PutawayThread | None = None

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        file_group = QGroupBox("表格文件（本地 CSV）")
        file_layout = QHBoxLayout(file_group)
        self.csv_path_edit = QLineEdit()
        self.csv_path_edit.setReadOnly(True)
        self.csv_path_edit.setPlaceholderText("请选择 CSV 文件…")
        self.btn_pick_csv = QPushButton("选择文件…")
        self.btn_pick_csv.clicked.connect(self._on_pick_csv)
        file_layout.addWidget(self.csv_path_edit, 1)
        file_layout.addWidget(self.btn_pick_csv)
        root.addWidget(file_group)

        out_group = QGroupBox("输出目录")
        out_layout = QHBoxLayout(out_group)
        self.output_dir_edit = QLineEdit()
        self.output_dir_edit.setPlaceholderText("选择表格后将自动设为表格所在目录")
        self.btn_pick_out = QPushButton("浏览…")
        self.btn_pick_out.clicked.connect(self._on_pick_output_dir)
        out_layout.addWidget(self.output_dir_edit, 1)
        out_layout.addWidget(self.btn_pick_out)
        root.addWidget(out_group)

        ctrl_layout = QHBoxLayout()
        self.btn_start_stop = QPushButton("开始")
        self.btn_start_stop.setEnabled(False)
        self.btn_start_stop.clicked.connect(self._on_start_stop)
        ctrl_layout.addWidget(self.btn_start_stop)
        ctrl_layout.addStretch(1)
        root.addLayout(ctrl_layout)

        log_header = QHBoxLayout()
        log_header.addWidget(QLabel("运行日志"))
        btn_clear_log = QPushButton("清空日志")
        btn_clear_log.clicked.connect(self._on_clear_log)
        log_header.addStretch(1)
        log_header.addWidget(btn_clear_log)
        root.addLayout(log_header)

        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        mono = QFont("Consolas")
        if not mono.exactMatch():
            mono = QFont("Courier New")
        self.log_view.setFont(mono)
        root.addWidget(self.log_view, 1)

        self.append_log("就绪。请选择 CSV 表格文件。")

    def append_log(self, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_view.appendPlainText(f"[{ts}] {message}")
        bar = self.log_view.verticalScrollBar()
        bar.setValue(bar.maximum())

    def closeEvent(self, event: QCloseEvent) -> None:
        if self._worker is not None and self._worker.isRunning():
            self.append_log("关闭窗口：请求停止任务…")
            if self._stop_event is not None:
                self._stop_event.set()
            self._worker.wait(5000)
        event.accept()

    def _set_busy(self, busy: bool) -> None:
        self._is_running = busy
        self.btn_pick_csv.setEnabled(not busy)
        self.btn_pick_out.setEnabled(not busy)
        self.btn_start_stop.setText("停止" if busy else "开始")
        self.btn_start_stop.setEnabled(busy or self._csv_path is not None)

    def _on_pick_csv(self) -> None:
        path, _ = QFileDialog.getOpenFileName(
            self,
            "选择 CSV 表格",
            "",
            "CSV 文件 (*.csv);;所有文件 (*.*)",
        )
        if not path:
            return
        p = Path(path)
        if not p.is_file():
            QMessageBox.warning(self, "提示", "所选路径不是有效文件。")
            return
        self._csv_path = p.resolve()
        self.csv_path_edit.setText(str(self._csv_path))
        out_dir = str(self._csv_path.parent)
        self.output_dir_edit.setText(out_dir)
        self.append_log(f"已选择表格：{self._csv_path}")
        self.append_log(f"输出目录已设为：{out_dir}")
        if not self._is_running:
            self.btn_start_stop.setEnabled(True)

    def _on_pick_output_dir(self) -> None:
        current = self.output_dir_edit.text().strip()
        start_dir = current if current and Path(current).is_dir() else str(Path.home())
        path = QFileDialog.getExistingDirectory(self, "选择输出目录", start_dir)
        if not path:
            return
        self.output_dir_edit.setText(path)
        self.append_log(f"输出目录已手动设为：{path}")

    def _on_worker_log(self, text: str) -> None:
        self.append_log(text)

    def _on_worker_finished(self, ok: bool, msg: str) -> None:
        self._worker = None
        self._stop_event = None
        self._set_busy(False)
        if ok:
            self.append_log(f"任务结束：{msg}")
        else:
            self.append_log(f"任务失败：{msg}")
            QMessageBox.warning(self, "任务失败", msg)

    def _on_start_stop(self) -> None:
        if not self._is_running:
            if self._csv_path is None or not self._csv_path.is_file():
                QMessageBox.warning(self, "提示", "请先选择有效的 CSV 文件。")
                return
            out = self.output_dir_edit.text().strip()
            out_path = Path(out)
            if not out_path.is_dir():
                QMessageBox.warning(self, "提示", "请选择有效的输出目录。")
                return
            self._stop_event = threading.Event()
            self._worker = PutawayThread(self._csv_path, out_path, self._stop_event)
            self._worker.log_message.connect(self._on_worker_log)
            self._worker.finished.connect(self._on_worker_finished)
            self._set_busy(True)
            self.append_log("开始处理表格…")
            self._worker.start()
        else:
            if self._stop_event is not None:
                self._stop_event.set()
                self.append_log("已请求停止：当前文件写完后结束。")

    def _on_clear_log(self) -> None:
        self.log_view.clear()
        self.append_log("日志已清空。")


def main() -> None:
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
