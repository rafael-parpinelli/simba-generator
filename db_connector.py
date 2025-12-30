import psycopg2
from psycopg2 import OperationalError

# ============================
# 🔗 Função de Conexão
# ============================
def conectar(host, port, dbname, user, password):
    """
    Faz conexão com o banco PostgreSQL.
    Retorna o objeto de conexão se for bem-sucedida.
    Lança exceção detalhada se falhar (sem encerrar o app).
    """
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        return conn
    except OperationalError as e:
        # 🔹 Retorna erro descritivo do PostgreSQL (mantém a mensagem original)
        raise Exception(str(e))
    except Exception as e:
        # 🔹 Captura erros genéricos (ex: host inválido)
        raise Exception(f"Erro inesperado na conexão: {e}")

# ============================
# 🧭 Listar bancos disponíveis
# ============================
def get_databases(conn):
    """
    Retorna lista de bancos de dados (exceto templates).
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname;")
        bancos = [r[0] for r in cur.fetchall()]
        cur.close()
        return bancos
    except Exception as e:
        print(f"⚠️ Erro ao listar bancos: {e}")
        return []


# ============================
# 🧭 Listar schemas
# ============================
def get_schemas(conn):
    """
    Retorna lista de schemas disponíveis (exclui pg_* e information_schema).
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT LIKE 'pg_%'
              AND schema_name NOT IN ('information_schema')
            ORDER BY schema_name;
        """)
        schemas = [r[0] for r in cur.fetchall()]
        cur.close()
        return schemas
    except Exception as e:
        print(f"⚠️ Erro ao listar schemas: {e}")
        return []


# ============================
# 📜 Executar consultas SQL
# ============================
def executar_query(conn, query):
    """
    Executa uma consulta SQL e retorna os resultados como lista de tuplas.
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.description:
                rows = cur.fetchall()
            else:
                conn.commit()
                rows = []
            return rows
    except Exception as e:
        print(f"⚠️ Erro ao executar query:\n{query}\n→ {e}")
        conn.rollback()
        return []

#Criar tabelas para geração do SIMBA
    
SIMBA_CODIGOS = [
##=========================
## DÉBITOS (D)
## =========================
(101, 'Cheque compensado', 'D'),
(102, 'Encargos', 'D'),
(103, 'Estornos', 'D'),
(104, 'Lançamento avisado', 'D'),
(105, 'Tarifas', 'D'),
(106, 'Aplicação', 'D'),
(107, 'Empréstimo / Financiamento', 'D'),
(108, 'Câmbio', 'D'),
(109, 'CPMF', 'D'),
(110, 'IOF', 'D'),
(111, 'Imposto de renda', 'D'),
(112, 'Pagamento a fornecedores', 'D'),
(113, 'Pagamento de salários', 'D'),
(114, 'Saque eletrônico', 'D'),
(115, 'Ações', 'D'),
(117, 'Transferência entre contas', 'D'),
(118, 'Devolução da compensação', 'D'),
(119, 'Devolução de cheque depositado', 'D'),
(120, 'Transferência interbancária (DOC, TED, Pix)', 'D'),
(121, 'Antecipação a fornecedores', 'D'),
(122, 'OC / AEROPS', 'D'),
(123, 'Saque em espécie', 'D'),
(124, 'Cheque pago', 'D'),
(125, 'Pagamentos diversos', 'D'),
(126, 'Pagamento de tributos', 'D'),
(127, 'Pagamento de fatura de cartão de crédito da própria IF', 'D'),

## =========================
## CRÉDITOS (C)
## =========================
(201, 'Depósito em cheque', 'C'),
(202, 'Crédito de cobrança', 'C'),
(203, 'Devolução de cheques', 'C'),
(204, 'Estornos', 'C'),
(205, 'Lançamento avisado', 'C'),
(206, 'Resgate de aplicação', 'C'),
(207, 'Empréstimo / Financiamento', 'C'),
(208, 'Câmbio', 'C'),
(209, 'Transferência interbancária (DOC, TED, Pix)', 'C'),
(210, 'Ações', 'C'),
(211, 'Dividendos', 'C'),
(212, 'Seguro', 'C'),
(213, 'Transferência entre contas', 'C'),
(214, 'Depósitos especiais', 'C'),
(215, 'Devolução da compensação', 'C'),
(216, 'OCT', 'C'),
(217, 'Pagamento de fornecedores', 'C'),
(218, 'Pagamentos diversos', 'C'),
(219, 'Recebimento de salários', 'C'),
(220, 'Depósito em espécie', 'C'),
(221, 'Recebimento de tributos', 'C'),
(222, 'Recebíveis de cartão de crédito', 'C'),
(223, 'Crédito Pix via QR Code', 'C')
    ]

def ensure_simba_tables(conn):
    ddl = [
        """
        create table if not exists public.simba_codigo (
            codigo_simba int primary key,
            descricao text not null,
            natureza char(1) not null check (natureza in ('D','C'))
        );
        """,
        """
        create table if not exists public.simba_depara_codhis (
            id serial primary key,
            codhis int not null,
            natureza char(1) not null check (natureza in ('D','C')),
            codigo_simba int not null,
            ativo boolean default true,
            constraint uk_codhis_natureza unique (codhis, natureza)
        );
        """
    ]

    with conn.cursor() as cur:
        for sql in ddl:
            cur.execute(sql)

    conn.commit()    

def load_simba_depara_padrao(conn):
    sql = """ INSERT INTO public.simba_depara_codhis (codhis, natureza, codigo_simba, ativo)
                VALUES
                -- =========================
                -- CRÉDITOS (C)
                -- =========================
                (22501, 'C', 209, true),
                (22504, 'C', 209, true),
                (22545, 'C', 209, true),
                (22521, 'C', 209, true),
                (11517, 'C', 209, true),

                (22598, 'C', 213, true),
                (22597, 'C', 213, true),
                (42,    'C', 213, true),
                (43,    'C', 213, true),

                (45,    'C', 220, true),
                (11501, 'C', 204, true),
                (22536, 'C', 204, true),
                -- =========================
                -- DÉBITOS (D)
                -- =========================
                (22544, 'D', 105, true),
                (62,    'D', 105, true),
                (11524, 'D', 105, true),
                (22600, 'D', 105, true),

                (80,    'D', 107, true),

                (42,    'D', 117, true),
                (22597, 'D', 117, true),
                (22599, 'D', 117, true),
                (43,    'D', 117, true),
                (1326,  'D', 117, true),
                (842,   'D', 117, true),

                (22500, 'D', 120, true),
                (11516, 'D', 120, true),

                (11500, 'D', 125, true),
                (11504, 'D', 126, true),
                (22524, 'D', 114, true)

                ON CONFLICT (codhis, natureza)
                DO NOTHING; """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def load_simba_codigos(conn):
    sql = """
    insert into public.simba_codigo (codigo_simba, descricao, natureza)
    values (%s, %s, %s)
    on conflict (codigo_simba) do nothing;
    """

    with conn.cursor() as cur:
        for row in SIMBA_CODIGOS:
            cur.execute(sql, row)

    conn.commit()

# ============================
# 🚨 Validação de CODHIS sem mapeamento SIMBA
# ============================
def validar_codhis_sem_mapeamento(conn, schema, agencia, contas, dt_ini, dt_fim):
    """
    Retorna a quantidade de CODHIS encontrados no período
    que NÃO possuem mapeamento ativo para o SIMBA.
    """
    with conn.cursor() as cur:
        # garante o schema correto
        cur.execute(f"SET search_path TO public, {schema};")

        cur.execute(
            """
            select count(distinct m.codhis)
            from mocc m
            where m.codage = %s
              and m.ctasoc = any(%s)
              and m.datmov between %s and %s
              and not exists (
                  select 1
                  from public.simba_depara_codhis d
                  where d.codhis = m.codhis
                    and d.ativo = true
              );
            """,
            (
                int(agencia),
                contas,
                dt_ini,
                dt_fim,
            )
        )

        qtd = cur.fetchone()[0]

    return qtd    