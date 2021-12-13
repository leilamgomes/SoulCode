-- Atividade 16 - Integração de diferentes databases via Python
-- João Victor Guimarães de Oliveira
-- Leila Moreira Gomes Roque
-- Robson Motta
-- ---------------------------------------------------------
-- Cria o Banco de Dados
CREATE DATABASE OLDTech_Ltda;

-- Seleciona o banco de dados
USE OLDTech_Ltda;

-- cria a tabela vendas
create table if not exists vendas(
		nota_fiscal integer auto_increment,
        nome_vendedor varchar(30) not null,
        total float not null,
        #Primary Key
        constraint dados_pkey primary key (nota_fiscal)
);