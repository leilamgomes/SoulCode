from modules.conector_cassandra import Interface_db_cassandra
from modules.conector_mysql import Interface_db_mysql
import pandas as pd

if __name__ == "__main__":
    try:
        #Importando os dados do arquivo CSV e salvando em um DataFrame
        Sistema_A_SQL = pd.read_csv("Sistema_A_SQL.csv", sep=",")
        Sistema_B_NoSQL = pd.read_csv("Sistema_B_NoSQL.csv", sep=",")
        
        #Excluindo a coluna nota_fiscal do DataFrame para inserção nos databases
        Sistema_A_SQL.drop(['nota_fiscal'], axis=1, inplace=True)
        Sistema_B_NoSQL.drop(['nota_fiscal'], axis=1, inplace=True)
        
        #Excluindo as linhas que contem campos vazios para inserção no database SQL
        Sistema_A_SQL = Sistema_A_SQL.dropna()
        
        #Instancia a interface para comunicação com os bancos de dados/keyspace já criados anteriormente
        interface_mysql = Interface_db_mysql("root","senha","127.0.0.1","OLDTech_Ltda") 
        interface_cassandra = Interface_db_cassandra('oldtech_ltda')
        
        #Insere os dados do DataFrame Sistema_A_SQL no banco de dados SQL - MySQL   
        for index, row in Sistema_A_SQL.iterrows():
            query=f"INSERT INTO vendas (nome_vendedor, total) VALUES ('{row['vendedor']}', {int(row['total'])})"
            interface_mysql.inserir(query)
                
        #Insere os dados do DataFrame Sistema_B_NoSQL no banco de dados NoSQL - Cassandra, verificando se existe algum campo nulo      
        for index, row in Sistema_B_NoSQL.iterrows():
            if pd.isnull( row['vendedor'] ):
                query=f"INSERT INTO vendas (nota_fiscal, total) VALUES ({'uuid()'},{int(row['total'])})"
                interface_cassandra.inserir(query)
            elif pd.isnull( row['total'] ):
                query=f"INSERT INTO vendas (nota_fiscal, nome_vendedor) VALUES ({'uuid()'},{int(row['vendedor'])})"
                interface_cassandra.inserir(query)
            else:
                query=f"INSERT INTO vendas (nota_fiscal, nome_vendedor, total) VALUES ({'uuid()'},'{row['vendedor']}', {int(row['total'])})"
                interface_cassandra.inserir(query)        
        
        #Consulta os dados do banco MySQL salvando em um DataFrame
        df_vendas = pd.DataFrame( interface_mysql.buscar("select * from vendas;") )
        df_vendas.columns = ['nota_fiscal','vendedor','total']
        
        #Insere os dados lidos do banco de dados SQL no banco de dados NoSQL    
        for index, row in df_vendas.iterrows():
            query=f"INSERT INTO vendas (nota_fiscal, nome_vendedor, total) VALUES ({'uuid()'},'{row['vendedor']}', {int(row['total'])})"
            interface_cassandra.inserir(query)
        
        #Bônus, código que salva um DataFrame em um arquivo .csv
        df_vendas.to_csv("df_vendas.csv", index = False, sep=',', encoding='utf-8')
                
        print("Fim da execução!")
        
    except Exception as e:
        print(str(e))