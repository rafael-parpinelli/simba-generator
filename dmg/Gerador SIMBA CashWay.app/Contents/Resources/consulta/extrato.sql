--#############################################################################
-- ARQUIVO: extrato.sql
-- GERAÇÃO DO ARQUIVO EXTRATO.TXT PARA O SIMBA
-- PLACEHOLDERS:
--   {{NUMERO_BANCO}}, {{AGENCIA_CLIENTE}}, {{CONTAS_CLIENTE}}, {{DATA_INICIAL}}, {{DATA_FINAL}}

DROP TABLE IF EXISTS PUBLIC.extrato_simba_3611_1;
select * into
public.extrato_simba_3611_1
from (

SELECT
rpad(CODIGO_CHAVE_EXTRATO::TEXT,18) as CODIGO_CHAVE_EXTRATO,
'{{NUMERO_BANCO}}',
left(NUMERO_AGENCIA::text,4) as NUMERO_AGENCIA,
NUMERO_CONTA,
TIPO_CONTA,
DATA_LANCAMENTO,
rpad(NUMERO_DOCUMENTO::text,5) as NUMERO_DOCUMENTO,
rpad(DESCRICAO_LANCAMENTO,40) as DESCRICAO_LANCAMENTO,
TIPO_LANCAMENTO,
RPAD(VALOR_LANCAMENTO::TEXT,14) AS VALOR_LANCAMENTO,
NATUREZA_LANCAMENTO,
VALOR_SALDO,
NATUREZA_SALDO,
LOCAL_TRANSACAO,
sequencia_ajj,
registroid
FROM (

SELECT 
*,
	replace(round(abs(SALDO),2)::text,'.','') AS VALOR_SALDO,
	(CASE WHEN SALDO < 0 THEN 'D' WHEN SALDO > 0 THEN 'C' ELSE 'C' END ) AS NATUREZA_SALDO



FROM (

SELECT *,
(SUM(VLRMOV_NAOVAI) OVER (ORDER BY ORDEM_NAOVAI)) AS SALDO

FROM (

SELECT
	cast(mocc.ctasoc as text)|| cast(mocc.id as text) AS CODIGO_CHAVE_EXTRATO,
	'{{NUMERO_BANCO}}' AS NUMERO_BANCO, 
	CODAGE AS NUMERO_AGENCIA,
	CTASOC AS NUMERO_CONTA,
	$$1$$ AS TIPO_CONTA, 
	format(DATMOV, 'ddmmyyyy') AS DATA_LANCAMENTO,
	CODHIS AS NUMERO_DOCUMENTO,
	(SELECT DESHIS FROM {{SCHEMA}}.HIST WHERE HIST.CODHIS = MOCC.CODHIS) AS DESCRICAO_LANCAMENTO,
	coalesce(
    (
        select d.codigo_simba::text
        from public.simba_depara_codhis d
        where
            d.codhis = mocc.codhis
            and d.natureza =
                case
                    when mocc.vlrmov < 0 then 'D'
                    else 'C'
                end
            and d.ativo = true
        limit 1
    ),
    '204'
) AS TIPO_LANCAMENTO, 
	replace(round(abs(VLRMOV),2)::text,'.','') AS VALOR_LANCAMENTO,
	(CASE WHEN VLRMOV < 0.00 THEN 'D' WHEN VLRMOV >= 0.00 THEN 'C' ELSE 'D' END ) AS NATUREZA_LANCAMENTO,
	(RANK()OVER (ORDER BY DATMOV, SEQREG))AS ORDEM_NAOVAI,
	'AGÊNCIA' AS LOCAL_TRANSACAO
	,ROUND(vlrmov,2) AS VLRMOV_NAOVAI,
	mocc.seqreg as sequencia_ajj,
	mocc.registroid
FROM {{SCHEMA}}.MOCC 
WHERE CODAGE = {{AGENCIA_CLIENTE}} AND CTASOC IN ({{CONTAS_CLIENTE}}) and
DATMOV BETWEEN '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
ORDER BY ctasoc,DATMOV,seqreg 
) AS A
) AS B
)AS C  order by numero_conta 
--where tipo_lancamento is null;
) AS D;


DROP TABLE IF EXISTS PUBLIC.extrato_simba_3611_1;
select * into
PUBLIC.extrato_simba_3611_1
from (

SELECT
rpad(CODIGO_CHAVE_EXTRATO::TEXT,18) as CODIGO_CHAVE_EXTRATO,
NUMERO_BANCO,
left(NUMERO_AGENCIA::text,4) as NUMERO_AGENCIA,
NUMERO_CONTA,
TIPO_CONTA,
DATA_LANCAMENTO,
rpad(NUMERO_DOCUMENTO::text,5) as NUMERO_DOCUMENTO,
rpad(DESCRICAO_LANCAMENTO,40) as DESCRICAO_LANCAMENTO,
TIPO_LANCAMENTO,
RPAD(VALOR_LANCAMENTO::TEXT,14) AS VALOR_LANCAMENTO,
NATUREZA_LANCAMENTO,
VALOR_SALDO,
NATUREZA_SALDO,
LOCAL_TRANSACAO,
sequencia_ajj,
registroid
FROM (

SELECT 
*,
	replace(round(abs(SALDO),2)::text,'.','') AS VALOR_SALDO,
	(CASE WHEN SALDO < 0 THEN 'D' WHEN SALDO > 0 THEN 'C' ELSE 'C' END ) AS NATUREZA_SALDO



FROM (

SELECT *,
(SUM(VLRMOV_NAOVAI) OVER (ORDER BY ORDEM_NAOVAI)) AS SALDO

FROM (

SELECT
	cast(mocc.ctasoc as text) || cast (mocc.id as text) AS CODIGO_CHAVE_EXTRATO,
	'{{NUMERO_BANCO}}' AS NUMERO_BANCO, 
	CODAGE AS NUMERO_AGENCIA,
	CTASOC AS NUMERO_CONTA,
	$$1$$ AS TIPO_CONTA, 
	format(DATMOV, 'ddmmyyyy') AS DATA_LANCAMENTO,
	CODHIS AS NUMERO_DOCUMENTO,
	(SELECT DESHIS FROM {{SCHEMA}}.HIST WHERE HIST.CODHIS = MOCC.CODHIS) AS DESCRICAO_LANCAMENTO,
	coalesce(
    (
        select d.codigo_simba::text
        from public.simba_depara_codhis d
        where
            d.codhis = mocc.codhis
            and d.natureza =
                case
                    when mocc.vlrmov < 0 then 'D'
                    else 'C'
                end
            and d.ativo = true
        limit 1
    ),
    '204'
) AS TIPO_LANCAMENTO, 
	replace(round(abs(VLRMOV),2)::text,'.','') AS VALOR_LANCAMENTO,
	(CASE WHEN VLRMOV < 0.00 THEN 'D' WHEN VLRMOV >= 0.00 THEN 'C' ELSE 'D' END ) AS NATUREZA_LANCAMENTO,
	(RANK()OVER (ORDER BY DATMOV, SEQREG))AS ORDEM_NAOVAI,
	'AGÊNCIA' AS LOCAL_TRANSACAO
	,ROUND(vlrmov,2) AS VLRMOV_NAOVAI,
	mocc.seqreg as sequencia_ajj,
	mocc.registroid
FROM {{SCHEMA}}.MOCC 
WHERE CODAGE = {{AGENCIA_CLIENTE}} AND CTASOC IN ({{CONTAS_CLIENTE}}) and
DATMOV BETWEEN '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}' 
ORDER BY ctasoc,DATMOV,seqreg 
) AS A
) AS B
)AS C  order by numero_conta 
--where tipo_lancamento is null;
) AS D;

--criar uma sequencia para ser usada no campo  CODIGO_CHAVE_EXTRATO
ALTER TABLE PUBLIC.extrato_simba_3611_1 add column chave serial;

select * from PUBLIC.extrato_simba_3611_1;  

select CODIGO_CHAVE_EXTRATO,
       NUMERO_BANCO,
       NUMERO_AGENCIA,
       NUMERO_CONTA,
	   TIPO_CONTA,	
	   DATA_LANCAMENTO,
	   NUMERO_DOCUMENTO,
	   DESCRICAO_LANCAMENTO,
	   coalesce(TIPO_LANCAMENTO::text,'204'), 
       VALOR_LANCAMENTO,
	   NATUREZA_LANCAMENTO,
	   VALOR_SALDO,
	   NATUREZA_SALDO,
 	   LOCAL_TRANSACAO
from PUBLIC.extrato_simba_3611_1;