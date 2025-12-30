# main.py
import sys
import threading
import os
from PyQt5 import QtWidgets, QtCore
from PyQt5.QtWidgets import (
    QApplication, QWidget, QLabel, QLineEdit, QPushButton,
    QFileDialog, QCheckBox, QTextEdit, QProgressBar, QMessageBox
)
from simba_generator import SimbaGenerator
from utils import save_config, load_config, log_message


class SimbaApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("SIMBA Generator - CashWay")
        self.setGeometry(300, 100, 750, 600)
        self.setStyleSheet("font-size: 13px;")
        self.config = load_config()
        self.setup_ui()

    def setup_ui(self):
        y = 20

        self.label_case = QLabel("Código do Caso (ex: 041-TST-007673-23):", self)
        self.label_case.move(20, y)
        self.input_case = QLineEdit(self)
        self.input_case.setText(self.config.get("case_code", ""))
        self.input_case.setGeometry(300, y, 300, 25)
        y += 40

        self.label_host = QLabel("Host:", self)
        self.label_host.move(20, y)
        self.input_host = QLineEdit(self)
        self.input_host.setText(self.config.get("host", ""))
        self.input_host.setGeometry(80, y, 200, 25)

        self.label_port = QLabel("Porta:", self)
        self.label_port.move(300, y)
        self.input_port = QLineEdit(self)
        self.input_port.setText(self.config.get("port", "5432"))
        self.input_port.setGeometry(350, y, 80, 25)
        y += 40

        self.label_user = QLabel("Usuário:", self)
        self.label_user.move(20, y)
        self.input_user = QLineEdit(self)
        self.input_user.setText(self.config.get("user", ""))
        self.input_user.setGeometry(80, y, 200, 25)

        self.label_pass = QLabel("Senha:", self)
        self.label_pass.move(300, y)
        self.input_pass = QLineEdit(self)
        self.input_pass.setEchoMode(QLineEdit.Password)
        self.input_pass.setText(self.config.get("password", ""))
        self.input_pass.setGeometry(350, y, 250, 25)
        y += 40

        self.label_db = QLabel("Database:", self)
        self.label_db.move(20, y)
        self.input_db = QLineEdit(self)
        self.input_db.setText(self.config.get("dbname", ""))
        self.input_db.setGeometry(90, y, 200, 25)
        y += 40

        self.label_output = QLabel("Pasta de Saída:", self)
        self.label_output.move(20, y)
        self.input_output = QLineEdit(self)
        self.input_output.setText(self.config.get("output_path", os.getcwd()))
        self.input_output.setGeometry(120, y, 400, 25)
        self.btn_browse = QPushButton("📂", self)
        self.btn_browse.setGeometry(530, y, 40, 25)
        self.btn_browse.clicked.connect(self.choose_output_folder)
        y += 40

        self.label_banks = QLabel("Selecionar Bancos:", self)
        self.label_banks.move(20, y)
        self.chk_coopcred = QCheckBox("Coopcred", self)
        self.chk_coopcred.move(150, y)
        self.chk_banking = QCheckBox("Banking", self)
        self.chk_banking.move(250, y)
        self.chk_coopcred.setChecked(True)
        self.chk_banking.setChecked(True)
        y += 40

        self.btn_generate = QPushButton("🚀 Gerar Arquivos SIMBA", self)
        self.btn_generate.setGeometry(20, y, 200, 40)
        self.btn_generate.clicked.connect(self.start_generation)

        self.progress = QProgressBar(self)
        self.progress.setGeometry(240, y + 10, 480, 25)
        y += 60

        self.log_box = QTextEdit(self)
        self.log_box.setGeometry(20, y, 700, 300)
        self.log_box.setReadOnly(True)

    def choose_output_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Selecionar Pasta de Saída")
        if folder:
            self.input_output.setText(folder)

    def log_to_ui(self, message):
        self.log_box.append(message)
        QtWidgets.QApplication.processEvents()

    def start_generation(self):
        case_code = self.input_case.text().strip()
        if not case_code:
            QMessageBox.warning(self, "Atenção", "Informe o código do caso (ex: 041-TST-007673-23).")
            return

        config = {
            "case_code": case_code,
            "host": self.input_host.text(),
            "port": self.input_port.text(),
            "user": self.input_user.text(),
            "password": self.input_pass.text(),
            "dbname": self.input_db.text(),
            "output_path": self.input_output.text(),
        }
        save_config(config)

        self.log_box.clear()
        self.progress.setValue(0)

        thread = threading.Thread(target=self.run_generator, args=(config,))
        thread.start()

    def run_generator(self, config):
        try:
            codigo_orgao, sigla, numero, dv = config["case_code"].split("-")

            gen = SimbaGenerator(
                codigo_orgao=codigo_orgao,
                sigla=sigla,
                numero=numero,
                dv=dv,
                host=config["host"],
                port=config["port"],
                user=config["user"],
                password=config["password"],
                dbname=config["dbname"],
                output_path=config["output_path"],
                coopcred=self.chk_coopcred.isChecked(),
                banking=self.chk_banking.isChecked(),
                log_callback=self.log_to_ui
            )

            self.progress.setValue(20)
            gen.run()
            self.progress.setValue(100)
            self.log_to_ui("✅ Processo concluído com sucesso!")

        except Exception as e:
            self.log_to_ui(f"❌ Erro: {str(e)}")
            QMessageBox.critical(self, "Erro", str(e))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SimbaApp()
    window.show()
    sys.exit(app.exec_())