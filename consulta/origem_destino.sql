--#############################################################################
-- ARQUIVO: origem_destino.sql
-- GERAÇÃO DO ARQUIVO ORIGEM_DESTINO.TXT PARA O SIMBA
-- PLACEHOLDERS:
--  {{NUMERO_BANCO}}, {{AGENCIA_CLIENTE}}, {{CONTAS_CLIENTE}}, {{DATA_INICIAL}}, {{DATA_FINAL}}


drop table if exists PUBLIC.ajj;

drop table if exists PUBLIC.lancamentos_normais;

select DISTINCT extrato_simba_3611_1.*  from PUBLIC.extrato_simba_3611_1;

select 
    CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PESSOAS_DOCUMENTOS.NUMERO_DOCUMENTO, E'\n',' '), E'\r',' '), '"',''), '''',''), '.',''), '/',''), '-','') AS NUMERO_DOCUMENTO_TRANSACAO,
	NUMERO_BANCO AS NUMERO_BANCO_OD,
	NUMERO_AGENCIA AS NUMERO_AGENCIA_OD,
	NUMERO_CONTA::text AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length(PESSOA.DOCUMENTO) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	PESSOA.DOCUMENTO AS CPF_CNPJ_OLD, 
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PESSOA.NOME, E'\n',' '), E'\r',' '), '"',''), '''',''), 'Ç', 'C'), 'Ã', 'A') AS NOME_PESSOA_OLD,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tipos_de_documento.descricao, E'\n',' '), E'\r',' '), '"',''), '''',''), 'Ç', 'C'), 'Ã', 'A') as NOME_DOC_IDENTIFICACAO_DOC,
	LEFT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE((pessoas_documentos.numero_documento||expedidor_documento), '/',''), '.', ''), '-', ''), E'\n',' '), E'\r',' '),20) AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
INTO PUBLIC.lancamentos_normais
from PUBLIC.extrato_simba_3611_1 as a
join  {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
left join (select * from {{SCHEMA}}.mocc where codhis in (42,43,9991,9992) and mocc.ctasoc  IN ({{CONTAS_CLIENTE}}) and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}') as transf on transf.ctasoc <> mocc.ctasoc and case when transf.seqreg <> 0 then transf.seqreg = mocc.seqreg else transf.registroid = mocc.registroid end
left join PUBLIC.contas_correntes on contas_correntes.conta = mocc.ctasoc and contas_correntes.agencia = mocc.codage
left join PUBLIC.pessoa on pessoa.id = contas_correntes.pessoa and pessoa.agencia_registro_id = contas_correntes.pessoa_agencia
left join PUBLIC.contas_correntes  contas_correntes2 on contas_correntes2.conta = transf.ctasoc and contas_correntes2.agencia = transf.codage
left join PUBLIC.pessoa pessoa2 on pessoa2.id = contas_correntes2.pessoa and pessoa2.agencia_registro_id = contas_correntes2.pessoa_agencia
left join PUBLIC.pessoas_documentos on pessoa.id = pessoas_documentos.pessoa_id and preferencial and pessoa.agencia_registro_id = pessoas_documentos.pessoa_agencia_registro_id
left join PUBLIC.tipos_de_documento on tipos_de_documento.id = pessoas_documentos.tipo_documento_id
where mocc.ctasoc  IN ({{CONTAS_CLIENTE}}) and mocc.codage = {{AGENCIA_CLIENTE}}
and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;



drop table if exists PUBLIC.trf_interna;

select  
	CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	a.NUMERO_DOCUMENTO AS NUMERO_DOCUMENTO_TRANSACAO,
	NUMERO_BANCO AS NUMERO_BANCO_OD,
	NUMERO_AGENCIA AS NUMERO_AGENCIA_OD,
	transf.ctasoc::text AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length(pessoa2.documento) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	pessoa2.documento AS CPF_CNPJ_OLD, 
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(PESSOA2.NOME, E'\n',' '), E'\r',' '), '"',''), '''',''), 'Ç', 'C'), 'Ã', 'A') AS NOME_PESSOA_OLD,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(tipos_de_documento.descricao, E'\n',' '), E'\r',' '), '"',''), '''',''), 'Ç', 'C'), 'Ã', 'A') as NOME_DOC_IDENTIFICACAO_DOC,
	LEFT(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE((pessoas_documentos.numero_documento||expedidor_documento), '/',''), '.', ''), '-', ''), E'\n',' '), E'\r',' '),20) AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
into PUBLIC.trf_interna
from PUBLIC.extrato_simba_3611_1 as a
join {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and mocc.codage = a.numero_agencia::int and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end and tabreg = 'MOCC' and mocc.codhis in (42,43,9991,9992,1326,1327,22597,22598,22599)
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
join (select * from {{SCHEMA}}.mocc where codhis in (42,43,9991,9992,1326,1327,22597,22598,22599) and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}') as transf on transf.ctasoc <> mocc.ctasoc and case when transf.seqreg <> 0 then transf.seqreg = mocc.seqreg else transf.registroid = mocc.registroid end
left join PUBLIC.contas_correntes on contas_correntes.conta = mocc.ctasoc and contas_correntes.agencia = mocc.codage
left join PUBLIC.pessoa on pessoa.id = contas_correntes.pessoa and pessoa.agencia_registro_id = contas_correntes.pessoa_agencia
left join PUBLIC.contas_correntes  contas_correntes2 on contas_correntes2.conta = transf.ctasoc and contas_correntes2.agencia = transf.codage
left join PUBLIC.pessoa pessoa2 on pessoa2.id = contas_correntes2.pessoa and pessoa2.agencia_registro_id = contas_correntes2.pessoa_agencia 
left join PUBLIC.pessoas_documentos on pessoa2.id = pessoas_documentos.pessoa_id and preferencial and pessoa.agencia_registro_id = pessoas_documentos.pessoa_agencia_registro_id
left join PUBLIC.tipos_de_documento on tipos_de_documento.id = pessoas_documentos.tipo_documento_id
and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
and(pessoa2.nome is not null)
where a.numero_documento::int in (42,43,9991,9992,1326,1327,22597,22598,22599)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;



drop table if exists PUBLIC.trf_saida;

select  
	CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	a.NUMERO_DOCUMENTO AS NUMERO_DOCUMENTO_TRANSACAO,
	coalesce(favoreds.bank_id,favoreds2.bank_id)::text AS NUMERO_BANCO_OD,
	coalesce(favoreds.agency,favoreds2.agency) AS NUMERO_AGENCIA_OD,
	coalesce(favoreds.account::text,favoreds2.account::text) AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length (coalesce(favoreds.document,favoreds2.document)) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	coalesce(favoreds.document,favoreds2.document) AS CPF_CNPJ_OLD, 
	coalesce(favoreds.name,favoreds2.name) AS NOME_PESSOA_OLD,
	$$ $$ AS NOME_DOC_IDENTIFICACAO_DOC,
	$$ $$ AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
INTO  PUBLIC.trf_saida	
from PUBLIC.extrato_simba_3611_1 as a
join {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and mocc.codage = a.numero_agencia::int and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end and tabreg = 'MOCC'
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
left join BANKING_DADOS.orders on (orders.id = mocc.ordem and orders.account::bigint = mocc.ctasoc and orders.type = 'TedTransfer') 
left join BANKING_DADOS.favoreds on favoreds.id = orders.favored_id
left join BANKING_DADOS.orders as orders2 on (mocc.vlrmov = -orders2.value and mocc.datmov = orders2.date and orders2.account::bigint = mocc.ctasoc and coalesce(mocc.ordem::text,'')= '') 
left join BANKING_DADOS.favoreds as favoreds2 on favoreds2.id = orders2.favored_id
where mocc.ctasoc IN ({{CONTAS_CLIENTE}}) and  mocc.codage = {{AGENCIA_CLIENTE}}
and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
and mocc.codhis not in (1325,11526,22500)
and (coalesce(favoreds.name, favoreds2.name) is not null)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;


drop table if exists PUBLIC.trf_entrada;

select  
	CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	a.NUMERO_DOCUMENTO AS NUMERO_DOCUMENTO_TRANSACAO,
	banks.code AS NUMERO_BANCO_OD,
	payer_agency AS NUMERO_AGENCIA_OD,
	payer_account::text AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length(payer_document) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	payer_document AS CPF_CNPJ_OLD, 
	payer_name AS NOME_PESSOA_OLD,
	'' as NOME_DOC_IDENTIFICACAO_DOC,
	'' AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
into PUBLIC.trf_entrada
from PUBLIC.extrato_simba_3611_1 as a
join {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and mocc.codage = a.numero_agencia::int and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end 
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
join BANKING_DADOS.cash_ins on cash_ins.id = mocc.cash_in_id and cash_ins.favored_account::bigint = mocc.ctasoc
join BANKING_DADOS.banks on UPPER(banks.title) = UPPER(cash_ins.payer_bank_name)
where mocc.ctasoc  IN ({{CONTAS_CLIENTE}}) and  mocc.codage = {{AGENCIA_CLIENTE}}
and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
and mocc.cash_in_id is not null
and a.numero_documento::bigint not in (22597,22598,22599)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by CODIGO_CHAVE_EXTRATO;


drop table if exists PUBLIC.str_transf;

select  
	CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	a.NUMERO_DOCUMENTO AS NUMERO_DOCUMENTO_TRANSACAO,
	banks.code AS NUMERO_BANCO_OD,
	source_agency AS NUMERO_AGENCIA_OD,
	source_account::text AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length(source_document) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	source_document AS CPF_CNPJ_OLD, 
	source_name AS NOME_PESSOA_OLD,
	'' as NOME_DOC_IDENTIFICACAO_DOC,
	'' AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
into PUBLIC.str_transf	
from PUBLIC.extrato_simba_3611_1 as a
join {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and mocc.codage = a.numero_agencia::int and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end 
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
join BANKING_DADOS.str_inputs on mocc.datmov = str_inputs.date and mocc.vlrmov = str_inputs.amount::double precision and mocc.commov = source_document and mocc.colmov = source_name
join BANKING_DADOS.banks on banks.ispb = str_inputs.source_institution_code
where mocc.ctasoc  IN ({{CONTAS_CLIENTE}}) and  mocc.codage = {{AGENCIA_CLIENTE}}
and mocc.datmov between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
and mocc.codhis not in (22597,22598,22599)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by CODIGO_CHAVE_EXTRATO;


drop table if exists PUBLIC.pix_saida;

select  
	CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	a.NUMERO_DOCUMENTO AS NUMERO_DOCUMENTO_TRANSACAO,
	coalesce(banks.code)::text AS NUMERO_BANCO_OD,
	coalesce(pf.agency) AS NUMERO_AGENCIA_OD,
	coalesce(pf.account::text) AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length (coalesce(pf.document)) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	coalesce(pf.document) AS CPF_CNPJ_OLD, 
	coalesce(pf.name) AS NOME_PESSOA_OLD,
	$$ $$ AS NOME_DOC_IDENTIFICACAO_DOC,
	$$ $$ AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
INTO PUBLIC.pix_saida	
from PUBLIC.extrato_simba_3611_1 as a
join {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and mocc.codage = a.numero_agencia::int and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end and tabreg = 'MOCC'
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
left join BANKING_DADOS.orders on (orders.id = mocc.ordem and orders.account::bigint = mocc.ctasoc and orders.type = 'PIX::Payment') 
left join BANKING_DADOS.pix_favoreds pf on pf.id = orders.pix_favored_id
left join BANKING_DADOS.pix_participants pp on pp.identifier::integer = pf.ispb::integer
left join BANKING_DADOS.banks on banks.code = pp.bank_code
where mocc.ctasoc IN ({{CONTAS_CLIENTE}}) and  mocc.codage = {{AGENCIA_CLIENTE}}
and mocc.datmov  between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
and pf.name <> ''
and mocc.codhis = 22500
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17;


drop table if exists PUBLIC.pix_entrada;

select  
	CODIGO_CHAVE_EXTRATO AS CODIGO_CHAVE_EXTRATO,
	VALOR_LANCAMENTO AS VALOR_TRANSACAO,
	a.NUMERO_DOCUMENTO AS NUMERO_DOCUMENTO_TRANSACAO,
	banks.code AS NUMERO_BANCO_OD,
	payer_agency AS NUMERO_AGENCIA_OD,
	payer_account::text AS NUMERO_CONTA_OD,
	TIPO_CONTA AS TIPO_CONTA_OD,
	(CASE WHEN char_length(payer_document) <= 11 THEN $$1$$ ELSE $$2$$ END) AS TIPO_PESSOA_OD,
	payer_document AS CPF_CNPJ_OLD, 
	payer_name AS NOME_PESSOA_OLD,
	'' as NOME_DOC_IDENTIFICACAO_DOC,
	'' AS NUMERO_DOC_IDENTIFICACAO_OD,
	$$ $$ AS CODIGO_DE_BARRAS,
	$$ $$ AS NOME_ENDOSSANTE_CHEQUE,
	$$ $$ AS DOC_ENDOSSANTE_CHEQUE,
	$$0$$ AS SITUACAO_IDENTIFICACAO,
	trim(DESCRICAO_LANCAMENTO) AS OBSERVACAO
into PUBLIC.pix_entrada	
from PUBLIC.extrato_simba_3611_1 as a
join {{SCHEMA}}.mocc on mocc.ctasoc = a.numero_conta and mocc.codage = a.numero_agencia::int and case when a.sequencia_ajj <> 0 then a.sequencia_ajj = mocc.seqreg else a.registroid = mocc.registroid end 
join {{SCHEMA}}.hist on hist.codhis = mocc.codhis
join BANKING_DADOS.cash_ins on cash_ins.id = mocc.cash_in_id and cash_ins.favored_account::bigint = mocc.ctasoc
join BANKING_DADOS.pix_participants pp on UPPER(pp.name) = UPPER(cash_ins.payer_bank_name)
join BANKING_DADOS.banks on banks.code = pp.bank_code
where mocc.ctasoc IN ({{CONTAS_CLIENTE}}) and  mocc.codage = {{AGENCIA_CLIENTE}}
and mocc.datmov  between '{{DATA_INICIAL}}' AND '{{DATA_FINAL}}'
and mocc.cash_in_id is not null
and mocc.codhis = 22504
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by CODIGO_CHAVE_EXTRATO;
												
commit;

drop table if exists PUBLIC.ajj;

select * from PUBLIC.lancamentos_normais;

select (ROW_NUMBER()OVER (ORDER BY CODIGO_CHAVE_EXTRATO))AS CODIGO_CHAVE_OD,* into public.ajj  from(

select * from PUBLIC.lancamentos_normais 
where not exists (select * from PUBLIC.trf_entrada where trf_entrada.CODIGO_CHAVE_EXTRATO::bigint = lancamentos_normais.CODIGO_CHAVE_EXTRATO::bigint ) 
  and not exists (select * from PUBLIC.trf_saida where trf_saida.CODIGO_CHAVE_EXTRATO::bigint = lancamentos_normais.CODIGO_CHAVE_EXTRATO ::bigint )
  and not exists (select * from PUBLIC.trf_interna where trf_interna.CODIGO_CHAVE_EXTRATO::bigint = lancamentos_normais.CODIGO_CHAVE_EXTRATO::bigint) 
  and not exists (select * from PUBLIC.pix_saida where pix_saida.CODIGO_CHAVE_EXTRATO::bigint = lancamentos_normais.CODIGO_CHAVE_EXTRATO ::bigint)
  and not exists (select * from PUBLIC.str_transf where str_transf.CODIGO_CHAVE_EXTRATO::bigint = lancamentos_normais.CODIGO_CHAVE_EXTRATO ::bigint)
  and not exists (select * from PUBLIC.pix_entrada where pix_entrada.CODIGO_CHAVE_EXTRATO::bigint = lancamentos_normais.CODIGO_CHAVE_EXTRATO::bigint)
	  
union all

select * from PUBLIC.trf_saida
where not exists (select * from PUBLIC.trf_entrada where trf_entrada.CODIGO_CHAVE_EXTRATO::bigint = trf_saida.CODIGO_CHAVE_EXTRATO::bigint)
  and not exists (select * from PUBLIC.pix_saida where pix_saida.CODIGO_CHAVE_EXTRATO ::bigint= trf_saida.CODIGO_CHAVE_EXTRATO ::bigint)
	
union all

select * from PUBLIC.trf_entrada
where not exists (select * from PUBLIC.pix_entrada where pix_entrada.CODIGO_CHAVE_EXTRATO::bigint = trf_entrada.CODIGO_CHAVE_EXTRATO::bigint) 
  and not exists (select * from PUBLIC.str_transf where str_transf.CODIGO_CHAVE_EXTRATO::bigint = trf_entrada.CODIGO_CHAVE_EXTRATO::bigint)	
	
union all

select * from PUBLIC.trf_interna

union all
	
select * from PUBLIC.pix_saida
where not exists (select * from PUBLIC.trf_interna where trf_interna.CODIGO_CHAVE_EXTRATO::bigint = pix_saida.CODIGO_CHAVE_EXTRATO::bigint) 
and not exists (select * from PUBLIC.str_transf where str_transf.CODIGO_CHAVE_EXTRATO ::bigint= pix_saida.CODIGO_CHAVE_EXTRATO::bigint )	

union all

select * from PUBLIC.pix_entrada
where not exists (select * from PUBLIC.trf_interna where trf_interna.CODIGO_CHAVE_EXTRATO::bigint= pix_entrada.CODIGO_CHAVE_EXTRATO::bigint) 
  and not exists (select * from PUBLIC.str_transf where str_transf.CODIGO_CHAVE_EXTRATO::bigint = pix_entrada.CODIGO_CHAVE_EXTRATO::bigint)	

union all	
	
select * from PUBLIC.str_transf	
where not exists (select * from PUBLIC.trf_entrada where trf_entrada.CODIGO_CHAVE_EXTRATO::bigint = str_transf.CODIGO_CHAVE_EXTRATO::bigint) 
  and not exists (select * from PUBLIC.pix_saida where pix_saida.CODIGO_CHAVE_EXTRATO::bigint = str_transf.CODIGO_CHAVE_EXTRATO::bigint)
  and not exists (select * from PUBLIC.pix_entrada where pix_entrada.CODIGO_CHAVE_EXTRATO::bigint = str_transf.CODIGO_CHAVE_EXTRATO::bigint)  
	
	
order by CODIGO_CHAVE_EXTRATO
) as a
GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18;

select * from PUBLIC.ajj;