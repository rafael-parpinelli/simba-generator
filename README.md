# Gerador SIMBA CashWay

Aplicação desktop para **macOS**, desenvolvida em **Python + PyQt5**, destinada à **geração, validação e gerenciamento de arquivos SIMBA**, atendendo às exigências do **Projeto SIMBA (MPF)**.

O sistema automatiza a preparação do banco de dados, valida códigos de histórico (CODHIS), permite configuração manual quando necessário e gera os arquivos finais de forma segura, auditável e reutilizável em múltiplos bancos.

---

## Funcionalidades

- Interface gráfica desktop (PyQt5)
- Conexão com PostgreSQL
- Criação automática das tabelas SIMBA (caso não existam)
- Carga automática da tabela SIMBA_CODIGOS
- Carga do DE/PARA padrão
- Validação de CODHIS pendentes
- Destaque visual apenas para itens não mapeados
- Tela dedicada para configuração manual de CODHIS
- Retomada automática da geração após ajustes
- Geração de arquivos SIMBA por período
- Suporte a múltiplos bancos de dados
- Logs persistentes por usuário
- Build nativo para macOS (.app e .dmg)

---

## Requisitos

- macOS (Apple Silicon ou Intel)
- Python 3.11
- PostgreSQL
- Virtualenv (recomendado)

---

## Instalação (ambiente de desenvolvimento)

```bash
python3.11 -m venv venv311
source venv311/bin/activate
pip install -r requirements.txt
```

---

## Executando localmente

```bash
python app.py
```

---

## Logs

Os logs de erro são gravados automaticamente em:

~/.simba_cashway/erro.log

Esse caminho evita falhas de execução em ambientes empacotados (.app).

---

## Banco de Dados

O sistema não exige setup manual.

Ao iniciar a aplicação, para cada banco conectado, ocorre automaticamente:

- Criação das tabelas SIMBA (ensure_simba_tables)
- Carga da tabela SIMBA_CODIGOS
- Carga do DE/PARA padrão
- Validação de CODHIS utilizados nas movimentações

Caso existam códigos não mapeados, o sistema bloqueia a geração até correção.

---

## Build macOS

Gerar aplicativo (.app):

```bash
chmod +x scripts/build_mac.sh
scripts/build_mac.sh
```

Gerar instalador (.dmg):

```bash
chmod +x scripts/build_dmg.sh
scripts/build_dmg.sh
```

---

## Versionamento

Diretórios que não devem ser versionados:

- venv*
- build/
- dist/
- output/
- *.dmg
- *.app
