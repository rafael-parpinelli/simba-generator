import os
from datetime import datetime
import re
import sys
import os

def resource_path(relative_path):
    """
    Retorna o caminho correto para arquivos quando rodando via PyInstaller
    """
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)

def substituir_placeholders(sql_texto, variaveis):
    """
    Substitui placeholders {{VAR}} ou $$VAR$$ com tratamento de aspas inteligente.
    - Strings recebem aspas simples.
    - Números ficam sem aspas.
    - Schema é inserido sem aspas.
    - Listas (ex: contas) são convertidas para '1','2','3'.
    """
    for chave, valor in variaveis.items():
        if valor is None:
            valor = ''
        valor_str = str(valor).strip()

        # Se for o schema, nunca usa aspas
        if chave.upper() == "SCHEMA":
            sql_texto = sql_texto.replace(f"{{{{{chave}}}}}", valor_str)
            sql_texto = sql_texto.replace(f"$${chave}$$", valor_str)
            continue

        # Se for lista de contas separadas por vírgula
        if ',' in valor_str:
            lista_formatada = ','.join([f"'{v.strip()}'" for v in valor_str.split(',') if v.strip()])
            sql_texto = sql_texto.replace(f"{{{{{chave}}}}}", lista_formatada)
            sql_texto = sql_texto.replace(f"$${chave}$$", lista_formatada)
            continue

        # Se for número puro, sem aspas
        if valor_str.isdigit():
            sql_texto = sql_texto.replace(f"{{{{{chave}}}}}", valor_str)
            sql_texto = sql_texto.replace(f"$${chave}$$", valor_str)
        else:
            # String comum — adiciona aspas simples, exceto datas ISO
            if re.match(r"^\d{4}-\d{2}-\d{2}$", valor_str):  # formato AAAA-MM-DD
                # data, não adiciona aspas
                pass
            elif not (valor_str.startswith("'") and valor_str.endswith("'")):
                valor_str = f"'{valor_str}'"
            sql_texto = sql_texto.replace(f"{{{{{chave}}}}}", valor_str)
            sql_texto = sql_texto.replace(f"$${chave}$$", valor_str)

    return sql_texto


# utils.py
import os
from datetime import datetime

def salvar_log(conteudo, pasta_saida):
    os.makedirs(pasta_saida, exist_ok=True)

    caminho = os.path.join(
        pasta_saida,
        f"log_execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    with open(caminho, "w", encoding="utf-8") as f:
        f.write(conteudo)

    return caminho

