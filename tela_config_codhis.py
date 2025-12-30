from PyQt5 import QtWidgets, QtGui, QtCore


class TelaConfigCodhis(QtWidgets.QDialog):
    def __init__(self, conn, schema, agencia, contas, data_ini, data_fim):
        super().__init__()

        self.conn = conn
        self.schema = schema
        self.agencia = agencia
        self.contas = contas
        self.data_ini = data_ini
        self.data_fim = data_fim
        self.configuracao_salva = False

        self.setWindowTitle("Configuração CODHIS → SIMBA")
        self.resize(900, 400)

        self._carregar_codigos_simba()
        self._criar_tabela()
        self._carregar_dados()

    # -------------------------------------------------

    def _carregar_codigos_simba(self):
        cur = self.conn.cursor()
        cur.execute(
            "select codigo_simba, descricao from public.simba_codigo order by codigo_simba"
        )
        self.codigos_simba = cur.fetchall()
        cur.close()

    # -------------------------------------------------

    def _criar_tabela(self):
        self.table = QtWidgets.QTableWidget()
        self.table.setColumnCount(5)
        self.table.setHorizontalHeaderLabels([
            "CODHIS",
            "DESCRIÇÃO HISTÓRICO",
            "NATUREZA",
            "CÓDIGO SIMBA",
            "DESCRIÇÃO SIMBA",
        ])

        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(self.table)

        btn = QtWidgets.QPushButton("Salvar")
        btn.clicked.connect(self._salvar)
        layout.addWidget(btn)

    def _atualizar_descricao(self, row, combo):
        codigo = combo.currentData()
        if not codigo:
            self.table.item(row, 4).setText("")
            return

        descricao = next(d for c, d in self.codigos_simba if c == codigo)
        self.table.item(row, 4).setText(descricao)

    def _destacar_pendente(self, row):
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            if item is not None:
                item.setBackground(QtGui.QColor("#3a2f1b"))   # fundo âmbar escuro
                item.setForeground(QtGui.QColor("#ffd479"))   # texto claro  

    # -------------------------------------------------

    def _carregar_dados(self):
        cur = self.conn.cursor()

        contas_sql = ",".join(["%s"] * len(self.contas))

        sql = f"""
        select
            x.codhis,
            h.deshis,
            x.natureza,
            d.codigo_simba,
            s.descricao
        from (
            select distinct
                m.codhis,
                case when m.vlrmov < 0 then 'D' else 'C' end as natureza
            from {self.schema}.mocc m
            where
                m.codage = %s
                and m.ctasoc in ({contas_sql})
                and m.datmov between %s and %s
        ) x
        join {self.schema}.hist h on h.codhis = x.codhis
        left join public.simba_depara_codhis d
            on d.codhis = x.codhis
        and d.natureza = x.natureza
        and d.ativo = true
        left join public.simba_codigo s
            on s.codigo_simba = d.codigo_simba
        order by x.codhis
        """

        params = [self.agencia, *self.contas, self.data_ini, self.data_fim]
        cur.execute(sql, params)
        dados = cur.fetchall()
        cur.close()

        self.table.setRowCount(len(dados))

        for row, (codhis, deshis, natureza, cod_simba, desc_simba) in enumerate(dados):
            self.table.setItem(row, 0, QtWidgets.QTableWidgetItem(str(codhis)))
            self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(deshis))
            self.table.setItem(row, 2, QtWidgets.QTableWidgetItem(natureza))

            combo = QtWidgets.QComboBox()
            combo.addItem("", None)

            for c, d in self.codigos_simba:
                combo.addItem(f"{c} - {d}", c)

            if cod_simba:
                idx = combo.findData(cod_simba)
                combo.setCurrentIndex(idx)

            combo.currentIndexChanged.connect(
                lambda _, r=row, cb=combo: self._atualizar_descricao(r, cb)
            )

            self.table.setCellWidget(row, 3, combo)
            self.table.setItem(row, 4, QtWidgets.QTableWidgetItem(desc_simba or ""))

            # 🎯 destaque só se pendente
            if not cod_simba:
                self._destacar_pendente(row)

    # -------------------------------------------------

    def _editar_linha(self, row, col):
        if col != 3:
            return

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Selecionar código SIMBA")

        layout = QtWidgets.QVBoxLayout(dlg)

        combo = QtWidgets.QComboBox()
        combo.addItems([f"{c} - {d}" for c, d in self.codigos_simba])
        layout.addWidget(combo)

        btn = QtWidgets.QPushButton("OK")
        layout.addWidget(btn)

        def confirmar():
            codigo = int(combo.currentText().split(" - ")[0])
            descricao = next(d for c, d in self.codigos_simba if c == codigo)

            self.table.setItem(row, 3, QtWidgets.QTableWidgetItem(str(codigo)))
            self.table.setItem(row, 4, QtWidgets.QTableWidgetItem(descricao))

            for c in range(5):
                self.table.item(row, c).setBackground(QtGui.QColor("#e6ffe6"))

            dlg.accept()

        btn.clicked.connect(confirmar)
        dlg.exec_()

    #-------------------------------------------------
    # SALVAR CONFIGURACOES DE PARA
    #-------------------------------------------------
    def _salvar(self):
        cur = self.conn.cursor()

        for row in range(self.table.rowCount()):
            codhis = int(self.table.item(row, 0).text())
            natureza = self.table.item(row, 2).text()

            combo = self.table.cellWidget(row, 3)
            if combo is None:
                continue

            codigo_simba = combo.currentData()
            if not codigo_simba:
                continue

            cur.execute(
                """
                insert into public.simba_depara_codhis (codhis, natureza, codigo_simba, ativo)
                values (%s, %s, %s, true)
                on conflict (codhis, natureza)
                do update set
                    codigo_simba = excluded.codigo_simba,
                    ativo = true
                """,
                (codhis, natureza, codigo_simba),
            )

        self.conn.commit()

        QtWidgets.QMessageBox.information(
            self,
            "Configuração salva",
            "Mapeamento CODHIS → SIMBA salvo com sucesso."
        )

        self.configuracao_salva = True
        self.close()