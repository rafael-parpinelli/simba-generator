import os
import pandas as pd
from zipfile import ZipFile

# ============================================================
# CONFIGURAÇÃO — defina os dois diretórios de origem
# ============================================================
PASTA1 = r"/Users/rafaelparpinelli/Documents/Scripts/simba_generator/output/Final/035-SSPBA-000299-14_20251030_113222"
PASTA2 = r"/Users/rafaelparpinelli/Documents/Scripts/simba_generator/output/Final/035-SSPBA-000299-14_20251030_125913"
SAIDA_DIR = os.path.join(os.getcwd(), "simba_merged")
os.makedirs(SAIDA_DIR, exist_ok=True)

# Mapeamento dos tipos de arquivo do SIMBA
GRUPOS = {
    "AGENCIAS": "AGENCIAS",
    "CONTAS": "CONTAS",
    "EXTRATO": "EXTRATO",
    "ORIGEM_DESTINO": "ORIGEM_DESTINO",
    "TITULARES": "TITULARES"
}

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def buscar_arquivos(tipo):
    """Busca o arquivo do tipo indicado nas duas pastas."""
    arquivos = []
    for pasta in [PASTA1, PASTA2]:
        for nome in os.listdir(pasta):
            if nome.endswith(".txt") and tipo in nome:
                arquivos.append(os.path.join(pasta, nome))
    return arquivos

def merge_arquivos(tipo, arquivos):
    """Concatena os arquivos de um mesmo tipo e ajusta sequencial para ORIGEM_DESTINO."""
    if not arquivos:
        print(f"⚠️ Nenhum arquivo encontrado para {tipo}")
        return None

    dfs = []
    for caminho in arquivos:
        print(f"📄 Lendo {caminho} ...")
        df = pd.read_csv(caminho, sep="\t", header=None, dtype=str, engine="python", quoting=3)
        dfs.append(df)

    merged = pd.concat(dfs, ignore_index=True)

    # Se for o arquivo ORIGEM_DESTINO, ajustar o sequencial CODIGO_CHAVE_OD
    if tipo == "ORIGEM_DESTINO":
        merged.iloc[:, 0] = range(1, len(merged) + 1)

    arquivo_saida = os.path.join(SAIDA_DIR, f"035-SSPBA-000299-14_{tipo}.txt")
    merged.to_csv(arquivo_saida, sep="\t", index=False, header=False)
    print(f"✅ Gerado: {arquivo_saida}")
    return arquivo_saida

# ============================================================
# EXECUÇÃO PRINCIPAL
# ============================================================
if __name__ == "__main__":
    print("🔹 Iniciando junção dos arquivos SIMBA...\n")
    arquivos_gerados = []

    for tipo in GRUPOS.keys():
        arquivos = buscar_arquivos(tipo)
        saida = merge_arquivos(tipo, arquivos)
        if saida:
            arquivos_gerados.append(saida)

    if arquivos_gerados:
        zip_path = os.path.join(os.getcwd(), "SIMBA_035-SSPBA-000299-14.zip")
        with ZipFile(zip_path, "w") as zipf:
            for arq in arquivos_gerados:
                zipf.write(arq, os.path.basename(arq))
        print(f"\n📦 ZIP final criado: {zip_path}")
    else:
        print("Nenhum arquivo foi gerado.")