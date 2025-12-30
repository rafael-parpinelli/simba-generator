--#############################################################################
-- ARQUIVO: agencias.sql
-- GERAÇÃO DO ARQUIVO AGENCIAS.TXT PARA O SIMBA
-- PLACEHOLDERS:
--   {{NUMERO_BANCO}}      → Código da instituição (ex: 085)
--   {{AGENCIA_CLIENTE}}   → Agência a ser informada
-- OBS: Executar no schema do cliente que contém as tabelas AGEN e PARA.

SELECT
    '{{NUMERO_BANCO}}' AS NUMERO_BANCO,
    LEFT(CODAGE::TEXT, 4) AS NUMERO_AGENCIA,
    NOMAGE AS NOME_AGENCIA,
    ENDAGE AS ENDERECO_LOGRADOURO,
    CIDAGE AS ENDERECO_CIDADE,
    SIGEST AS ENDERECO_UF,
    'BRASIL' AS ENDERECO_PAIS,
    CEPAGE AS ENDERECO_CEP,
    TELAGE AS TELEFONE_AGENCIA,
    TO_CHAR(DNIAGE, 'DDMMYYYY') AS DATA_ABERTURA,
    '' AS DATA_FECHAMENTO
FROM {{SCHEMA}}.AGEN
WHERE CODAGE = (SELECT CODAGE FROM {{SCHEMA}}.PARA);
