-- Atividade 16 - Integração de diferentes databases via Python
-- João Victor Guimarães de Oliveira
-- Leila Moreira Gomes Roque
-- Robson Motta
-- ---------------------------------------------------------
-- Cria a keyspace
CREATE KEYSPACE IF NOT EXISTS oldtech_ltda WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

-- Seleciona a keyspace
use oldtech_ltda;

-- cria a tabela vendas
create table if not exists "oldtech_ltda"."vendas"(
	nota_fiscal uuid primary key,
	nome_vendedor text,
	total float,
);