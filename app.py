import sys
from datetime import datetime
from db_connector import conectar, ensure_simba_tables, load_simba_codigos, load_simba_depara_padrao
from simba_generator import gerar_arquivos_simba
from utils import salvar_log, resource_path
from psycopg2 import OperationalError, DatabaseError
from tela_config_codhis import TelaConfigCodhis
import traceback
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QLabel,
    QLineEdit,
    QComboBox,
    QTextEdit,
    QProgressBar,
    QFileDialog,
    QCheckBox,
    QGroupBox,
    QFormLayout,
    QHBoxLayout
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QIcon
import logging
from pathlib import Path
import os

LOG_DIR = os.path.join(os.path.expanduser("~"), ".simba_cashway")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "erro.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s"
)

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def excecao_global(exctype, value, tb):
    erro = "".join(traceback.format_exception(exctype, value, tb))
    print(erro)
    try:
        QMessageBox.critical(None, "Erro crítico", erro)
    except Exception:
        pass

sys.excepthook = excecao_global

# ===============================================
# THREAD PARA EXECUTAR GERAÇÃO COM SINAL SEGURO
# ===============================================
class GeracaoThread(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    done_signal = pyqtSignal(str)

    def __init__(self, conn, schema, numero_banco, agencia, contas, data_inicio, data_fim, output_dir, consultas, prefixo):
        super().__init__()
        self.conn = conn
        self.schema = schema
        self.numero_banco = numero_banco
        self.agencia = agencia
        self.contas = contas
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.output_dir = output_dir
        self.consultas = consultas
        self.prefixo = prefixo

    def run(self):
        try:
            resultado = gerar_arquivos_simba(
                conn=self.conn,
                schema=self.schema,
                numero_banco=self.numero_banco,
                agencia_cliente=self.agencia,
                contas_cliente=self.contas,
                data_inicio=self.data_inicio,
                data_fim=self.data_fim,
                output_dir=self.output_dir,
                consultas=self.consultas,
                prefixo=self.prefixo,
                progress_callback=lambda i: self.progress_signal.emit(i)  # ✅ envia progresso
            )
            self.done_signal.emit(resultado)

        except Exception as e:
            import traceback
            erro = traceback.format_exc()
            self.done_signal.emit(f"🚨 Erro durante a execução:\n{erro}")

# ===============================================
# APLICAÇÃO PRINCIPAL
# ===============================================
class SimbaApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Gerador SIMBA – CashWay")
        self.setGeometry(200, 200, 800, 750)
        self.setWindowIcon(QIcon(resource_path("simba_icon.png")))

        layout = QVBoxLayout()
        self.setLayout(layout)

        # ===============================
        # CAMPOS DE CONEXÃO
        # ===============================
        form = QFormLayout()
        self.combo_banco = QComboBox()
        self.combo_banco.addItems(["coopcred", "banking"])
        form.addRow("Tipo de banco:", self.combo_banco)

        self.input_host = QLineEdit("localhost")
        form.addRow("Host:", self.input_host)

        self.input_port = QLineEdit("5432")
        form.addRow("Porta:", self.input_port)

        self.input_user = QLineEdit()
        form.addRow("Usuário:", self.input_user)

        self.input_password = QLineEdit()
        self.input_password.setEchoMode(QLineEdit.Password)
        form.addRow("Senha:", self.input_password)
        layout.addLayout(form)

        # ===============================
        # BOTÕES DE CONEXÃO E LISTAGEM
        # ===============================
        self.btn_conectar = QPushButton("Conectar e listar Bancos")
        self.btn_conectar.clicked.connect(self.listar_bancos)
        layout.addWidget(self.btn_conectar)

        self.combo_database = QComboBox()
        self.combo_database.currentIndexChanged.connect(self.listar_schemas)
        layout.addWidget(self.combo_database)

        self.combo_schema = QComboBox()
        layout.addWidget(self.combo_schema)

        # ===============================
        # CAMPOS DO PEDIDO JUDICIAL
        # ===============================
        form2 = QFormLayout()
        self.input_numero_banco = QLineEdit()
        form2.addRow("Número do Banco:", self.input_numero_banco)

        self.input_agencia = QLineEdit()
        form2.addRow("Agência:", self.input_agencia)

        self.input_contas = QLineEdit()
        form2.addRow("Contas (separadas por vírgula):", self.input_contas)

        self.input_data_inicio = QLineEdit()
        self.input_data_inicio.setPlaceholderText("AAAA-MM-DD")
        form2.addRow("Data Inicial:", self.input_data_inicio)

        self.input_data_fim = QLineEdit()
        self.input_data_fim.setPlaceholderText("AAAA-MM-DD")
        form2.addRow("Data Final:", self.input_data_fim)
        layout.addLayout(form2)

        # ===============================
        # CHECKBOXES DAS CONSULTAS
        # ===============================
        self.group_checks = QGroupBox("Consultas a Executar")
        checks_layout = QVBoxLayout()
        self.chk_agencias = QCheckBox("AGENCIAS")
        self.chk_contas = QCheckBox("CONTAS")
        self.chk_titulares = QCheckBox("TITULARES")
        self.chk_extrato = QCheckBox("EXTRATO")
        self.chk_origem = QCheckBox("ORIGEM_DESTINO")
        for c in [self.chk_agencias, self.chk_contas, self.chk_titulares, self.chk_extrato, self.chk_origem]:
            c.setChecked(True)
            checks_layout.addWidget(c)
        self.group_checks.setLayout(checks_layout)
        layout.addWidget(self.group_checks)

        # ===============================
        # CAMINHO DE SAÍDA
        # ===============================
        path_layout = QHBoxLayout()
        default_output = os.path.join(
            os.path.expanduser("~"),
            "Documents",
            "SIMBA_Output"
        )

        self.output_path = QLineEdit(default_output)
        self.btn_escolher_pasta = QPushButton("Selecionar pasta")
        self.btn_escolher_pasta.clicked.connect(self.selecionar_pasta)
        path_layout.addWidget(self.output_path)
        path_layout.addWidget(self.btn_escolher_pasta)
        layout.addLayout(path_layout)

        # ===============================
        # CAMPO DO PROCESSO (prefixo SIMBA)
        # ===============================
        form3 = QFormLayout()
        self.input_processo = QLineEdit()
        self.input_processo.setPlaceholderText("Ex: 041-TST-007673-23")
        form3.addRow("Número do Processo:", self.input_processo)
        layout.addLayout(form3)

        # ===============================
        # BOTÃO DE EXECUÇÃO
        # ===============================
        self.btn_gerar = QPushButton("🚀 Gerar Arquivos SIMBA")
        self.btn_gerar.clicked.connect(self.executar_geracao_thread)
        layout.addWidget(self.btn_gerar)

        self.btn_configurar = QPushButton("🧩 Configurar códigos SIMBA")
        self.btn_configurar.clicked.connect(self.abrir_configuracao_codigos)
        layout.addWidget(self.btn_configurar)
        # ===============================
        # BARRA DE PROGRESSO
        # ===============================
        self.progress = QProgressBar()
        self.progress.setValue(0)
        self.progress.setMaximum(5)
        layout.addWidget(self.progress)

        # ===============================
        # ÁREA DE LOG
        # ===============================
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        layout.addWidget(self.log_area)

        self.conn = None

    # ===============================
    # FUNÇÕES AUXILIARES
    # ===============================
    def selecionar_pasta(self):
        pasta = QFileDialog.getExistingDirectory(self, "Selecionar pasta de saída")
        if pasta:
            self.output_path.setText(pasta)

    def listar_bancos(self):
        host = self.input_host.text().strip()
        port = self.input_port.text().strip()
        user = self.input_user.text().strip()
        password = self.input_password.text().strip()

        self.log("🔌 Conectando ao banco padrão 'postgres'...")
        try:
            conn = conectar(host, port, "postgres", user, password)
            if not conn:
                raise OperationalError("Falha na conexão retornou None.")
        except Exception as e:
            erro = str(e).replace("\n", " ")
            self.log(f"❌ Erro ao conectar: {erro}")
            QMessageBox.critical(self, "Erro de Conexão", f"Não foi possível conectar:\n{erro}")
            return

        self.conn = conn
        self.log("✅ Conexão estabelecida com sucesso (banco: postgres).")

        try:
            cur = conn.cursor()
            cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;")
            bancos = [r[0] for r in cur.fetchall()]
            cur.close()
        except Exception as e:
            self.log(f"❌ Erro ao listar bancos: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao listar bancos:\n{e}")
            return

        self.combo_database.clear()
        self.combo_database.addItems(bancos)
        self.log(f"🏦 Bancos disponíveis: {', '.join(bancos)}")

        self.btn_conectar.setText("Selecionar banco e listar Schemas")
        self.btn_conectar.clicked.disconnect()
        self.btn_conectar.clicked.connect(self.listar_schemas)

    def listar_schemas(self):
        banco = self.combo_database.currentText()
        host = self.input_host.text().strip()
        port = self.input_port.text().strip()
        user = self.input_user.text().strip()
        password = self.input_password.text().strip()

        self.log(f"🔁 Conectando ao banco selecionado: {banco}")
        try:
            conn = conectar(host, port, banco, user, password)
            if not conn:
                raise OperationalError("Falha na conexão retornou None.")
        except Exception as e:
            erro = str(e).replace("\n", " ")
            self.log(f"❌ Erro ao conectar no banco {banco}: {erro}")
            QMessageBox.critical(self, "Erro de Conexão", f"Falha ao conectar no banco:\n{erro}")
            return

        self.conn = conn
        self.log(f"✅ Conectado ao banco {banco}.\n")

        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT LIKE 'pg_%' 
                  AND schema_name NOT IN ('information_schema');
            """)
            schemas = [r[0] for r in cur.fetchall()]
            cur.close()
        except Exception as e:
            self.log(f"❌ Erro ao listar schemas: {e}")
            QMessageBox.critical(self, "Erro", f"Erro ao listar schemas:\n{e}")
            return

        self.combo_schema.clear()
        self.combo_schema.addItems(schemas)
        self.log(f"✔️ Schemas disponíveis em {banco}: {', '.join(schemas)}")

    def validar_campos(self):
        obrigatorios = {
            "Número do Banco": self.input_numero_banco.text(),
            "Agência": self.input_agencia.text(),
            "Contas": self.input_contas.text(),
            "Data Inicial": self.input_data_inicio.text(),
            "Data Final": self.input_data_fim.text(),
            "Número do Processo": self.input_processo.text(),
        }
        faltando = [campo for campo, valor in obrigatorios.items() if not valor.strip()]
        if faltando:
            QMessageBox.warning(
                self, "Campos obrigatórios", f"Por favor, preencha: {', '.join(faltando)}"
            )
            return False
        return True

    def executar_geracao_thread(self):
        if not self.validar_campos():
            return

        prefixo = self.input_processo.text().strip().upper()
        schema = self.combo_schema.currentText()
        numero_banco = self.input_numero_banco.text().strip()
        agencia = self.input_agencia.text().strip()
        contas = self.input_contas.text().strip()
        data_inicio = self.input_data_inicio.text().strip()
        data_fim = self.input_data_fim.text().strip()
        output_dir = self.output_path.text().strip()

        consultas = []
        if self.chk_agencias.isChecked(): consultas.append("AGENCIAS")
        if self.chk_contas.isChecked(): consultas.append("CONTAS")
        if self.chk_titulares.isChecked(): consultas.append("TITULARES")
        if self.chk_extrato.isChecked(): consultas.append("EXTRATO")
        if self.chk_origem.isChecked(): consultas.append("ORIGEM_DESTINO")

        self.progress.setMaximum(len(consultas))
        self.progress.setValue(0)
        self.log(f"🚀 Iniciando geração dos arquivos SIMBA ({len(consultas)} consultas)...\n")

        # Chama a thread que executa todas as consultas de uma vez
        self.thread = GeracaoThread(
            self.conn, schema, numero_banco, agencia, contas, data_inicio, data_fim, output_dir, consultas, prefixo
        )
        self.thread.progress_signal.connect(self.progress.setValue)
        self.thread.done_signal.connect(self.finalizar_execucao)
        self.thread.start()
    
    
    def abrir_configuracao_codigos(self):
    # 🔒 valida campos mínimos
        if not self.input_agencia.text().strip():
            QMessageBox.warning(
                self,
                "Campo obrigatório",
                "Informe a Agência antes de configurar os códigos SIMBA."
            )
            return

        if not self.input_contas.text().strip():
            QMessageBox.warning(
                self,
                "Campo obrigatório",
                "Informe ao menos uma Conta antes de configurar os códigos SIMBA."
            )
            return

        if not self.input_data_inicio.text().strip() or not self.input_data_fim.text().strip():
            QMessageBox.warning(
                self,
                "Campo obrigatório",
                "Informe o período (Data Inicial e Final)."
            )
            return

        try:
            # 🔧 garante estrutura SIMBA
            ensure_simba_tables(self.conn)
            load_simba_codigos(self.conn)
            load_simba_depara_padrao(self.conn)


            tela = TelaConfigCodhis(
                conn=self.conn,
                schema=self.combo_schema.currentText(),
                agencia=int(self.input_agencia.text().strip()),  # 🔐 cast explícito
                contas=[c.strip() for c in self.input_contas.text().split(",")],
                data_ini=self.input_data_inicio.text().strip(),
                data_fim=self.input_data_fim.text().strip(),
            )

            tela.exec_()

            if tela.configuracao_salva:
                self.log("🔁 Configuração salva. Gere o SIMBA novamente.")

        except Exception as e:
            QMessageBox.critical(
                self,
                "Erro",
                str(e)
            )

    def finalizar_execucao(self, texto_log):
        self.log(texto_log)

        # 🚨 Detecta erro de CODHIS sem mapeamento
        if "sem mapeamento para o SIMBA" in texto_log:
            self.log("🧩 Abrindo tela de configuração de códigos SIMBA...")

            tela = TelaConfigCodhis(
                conn=self.conn,
                schema=self.combo_schema.currentText(),
                agencia=self.input_agencia.text().strip(),
                contas=[c.strip() for c in self.input_contas.text().split(",")],
                data_ini=self.input_data_inicio.text().strip(),
                data_fim=self.input_data_fim.text().strip(),
            )

            tela.exec_() if hasattr(tela, "exec_") else tela.show()

            # 🔁 Se o usuário salvou, tenta gerar novamente
            if getattr(tela, "configuracao_salva", False):
                self.log("🔁 Configuração salva. Reexecutando geração do SIMBA...\n")
                self.executar_geracao_thread()
                return

        # fluxo normal
        pasta_saida = self.output_path.text().strip()
        caminho_log = salvar_log(texto_log, pasta_saida)
        self.log(f"\n📄 Log completo salvo em: {caminho_log}")
        self.log("✅ Processo concluído.")

    def log(self, msg):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_area.append(f"[{timestamp}] {msg}")
        self.log_area.ensureCursorVisible()


if __name__ == "__main__":
    try:
        app = QApplication(sys.argv)
        janela = SimbaApp()
        janela.show()
        sys.exit(app.exec_())
    except Exception as e:
        import traceback
        erro = traceback.format_exc()
        with open("/tmp/simba_erro.log", "w") as f:
            f.write(erro)
        raise