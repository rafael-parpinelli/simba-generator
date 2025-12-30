# simba_generator.py
import os
from datetime import datetime
from zipfile import ZipFile
import pandas as pd
from utils import substituir_placeholders
from tela_config_codhis import TelaConfigCodhis
from db_connector import (
    ensure_simba_tables,
    load_simba_codigos,
    validar_codhis_sem_mapeamento,
    load_simba_depara_padrao
)
from utils import resource_path

# apague os .txt depois de zipar? (mude para False se quiser manter)
DELETE_TXT_AFTER_ZIP = True


def gerar_arquivos_simba(
    conn,
    schema,
    numero_banco,
    agencia_cliente,
    contas_cliente,
    data_inicio,
    data_fim,
    output_dir,
    consultas,
    prefixo,
    progress_callback=None,  # ✅ agora existe!
):
    """
    Executa as consultas SQL do SIMBA (AGENCIAS, CONTAS, TITULARES, EXTRATO, ORIGEM_DESTINO)
    e gera um ZIP único com todos os arquivos.

    progress_callback(i:int) pode ser passado para atualizar a barra de progresso na UI.
    """
    log_execucao = []
    cursor = conn.cursor()
    arquivos_gerados = []
    agencia_banco = agencia_cliente.strip()
    agencia_simba = agencia_banco[:4]
    # =========================================================
    # 🔧 GARANTE ESTRUTURA SIMBA (MULTI-BANCO)
    # =========================================================
    ensure_simba_tables(conn)
    load_simba_codigos(conn)
    load_simba_depara_padrao(conn)

    # =========================================================
    # 🚨 VALIDAÇÃO CRÍTICA – CODHIS SEM MAPEAMENTO
    # =========================================================
    pendentes = validar_codhis_sem_mapeamento(
        conn,
        schema=schema,
        agencia=agencia_banco,
        contas=[int(c.strip()) for c in contas_cliente.split(",")],
        dt_ini=data_inicio,
        dt_fim=data_fim
    )

    if pendentes > 0:
        raise Exception(
            f"Existem {pendentes} códigos de operação (CODHIS) "
            f"sem mapeamento para o SIMBA.\n\n"
            f"Acesse a configuração de códigos antes de gerar os arquivos."
        ) 


    consultas_map = {
        "AGENCIAS": resource_path("consulta/agencias.sql"),
        "CONTAS": resource_path("consulta/contas.sql"),
        "TITULARES": resource_path("consulta/titulares.sql"),
        "EXTRATO": resource_path("consulta/extrato.sql"),
        "ORIGEM_DESTINO": resource_path("consulta/origem_destino.sql"),
    }

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    try:
        total = len(consultas)
        for i, nome_consulta in enumerate(consultas, start=1):
            caminho_sql = consultas_map.get(nome_consulta)
            if not caminho_sql or not os.path.exists(caminho_sql):
                log_execucao.append(f"⚠️ Consulta {nome_consulta} não encontrada em {caminho_sql}")
                if progress_callback:
                    progress_callback(i)
                continue

            with open(caminho_sql, "r", encoding="utf-8") as f:
                sql = f.read()

            # placeholders -> valores (datas sem aspas; schema sem aspas)
            sql_exec = substituir_placeholders(
                sql,
                {
                    "NUMERO_BANCO": numero_banco.strip(),
                    "AGENCIA_CLIENTE": agencia_banco.strip(),
                    "CONTAS_CLIENTE": contas_cliente.strip(),
                    "DATA_INICIAL": data_inicio.strip().replace("'", ""),
                    "DATA_FINAL": data_fim.strip().replace("'", ""),
                    "SCHEMA": schema.strip(),
                },
            )

            # garantir search_path para resolver objetos sem schema
            cursor.execute(f"SET search_path TO {schema};")

            try:
                cursor.execute(sql_exec)

                # Se for SELECT, teremos description; senão é DDL/DML (commit e segue)
                if cursor.description is None:
                    conn.commit()
                    dados = []
                    colunas = []
                else:
                    dados = cursor.fetchall()
                    colunas = [d[0] for d in cursor.description]
                    if nome_consulta == "EXTRATO":
                        conn.commit()  # garante a existência de temporárias/auxiliares
                        log_execucao.append("⚙️ Tabela auxiliar/CTE de EXTRATO preparada.")

                if not dados:
                    log_execucao.append(f"⚠️ Nenhum dado retornado para {nome_consulta}")
                else:
                    df = pd.DataFrame(dados, columns=colunas)
                    # nome padrão SIMBA p/ cada arquivo
                    nome_txt = f"{prefixo}_{nome_consulta.upper()}.txt"
                    caminho_txt = os.path.join(output_dir, nome_txt)
                    df.to_csv(caminho_txt, sep="\t", index=False, header=False, encoding="utf-8")
                    arquivos_gerados.append(caminho_txt)
                    log_execucao.append(f"✅ {nome_consulta} gerado com sucesso ({len(df)} registros)")

            except Exception as e:
                log_execucao.append(f"❌ Erro ao executar {nome_consulta}: {e}")
                conn.rollback()

            # ✅ atualiza a barra a cada consulta
            if progress_callback:
                progress_callback(i)

        # ZIP único no final
        zip_nome = f"{prefixo}_{timestamp}.zip"
        zip_path = os.path.join(output_dir, zip_nome)
        with ZipFile(zip_path, "w") as zipf:
            for arq in arquivos_gerados:
                zipf.write(arq, os.path.basename(arq))
        log_execucao.append(f"📦 Arquivo ZIP final gerado: {zip_path}")

        if DELETE_TXT_AFTER_ZIP and arquivos_gerados:
            for arq in arquivos_gerados:
                try:
                    os.remove(arq)
                except Exception:
                    pass
            log_execucao.append("🧹 Arquivos TXT removidos após compactação.")

    except Exception as e:
    # 🚨 Erro de CODHIS sem mapeamento → abre tela de configuração
        if "sem mapeamento para o SIMBA" in str(e):
            try:
                tela = TelaConfigCodhis(
                    conn=conn,
                    schema=schema,
                    agencia=agencia_banco,
                    contas=[c.strip() for c in contas_cliente.split(",")],
                    data_ini=data_inicio,
                    data_fim=data_fim,
                )

                tela.wait_window()

                # 🔁 Se o usuário salvou, tenta gerar novamente
                if getattr(tela, "configuracao_salva", False):
                    return gerar_arquivos_simba(
                        conn,
                        schema,
                        numero_banco,
                        agencia_cliente,
                        contas_cliente,
                        data_inicio,
                        data_fim,
                        output_dir,
                        consultas,
                        prefixo,
                        progress_callback,
                    )

            except Exception as tela_err:
                log_execucao.append(f"⚠️ Erro ao abrir tela de configuração: {tela_err}")

        log_execucao.append(f"🚨 Erro geral: {e}")

    cursor.close()

    return "\n".join(log_execucao)