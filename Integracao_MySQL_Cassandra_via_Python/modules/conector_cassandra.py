from cassandra.cluster import Cluster

class Interface_db_cassandra():
    cluster = ""
    session = ""

    def __init__(self, database = "soulcode"):
        """Construtor da classe Interface_db_cassandra

        Args:
            database (string, optional): nome da database. Defaults to "soulcode".
        """
        try:
            self.cluster = Cluster()
            self.set_session(database)   
        except Exception as e:
            print(str(e))
            
    def set_session(self, database):
        """Realiza a conexão com a database

        Args:
            database (string): nome da database
        """
        try:
            self.session=self.cluster.connect(database)
        except Exception as e:
            print(str(e))
            
    def fetchall(self, dados):
        """Função que retorna uma lista a partir 
           de um objeto da classe Cluster

        Args:
            dados (Object Cluster): Objeto cluster 
                com os dados buscados do banco de dados
        Returns:
            list : Lista com os dados lidos do banco de dados
        """
        try:
            lista = []
            for d in dados:
                lista.append(d)
            return lista
        except Exception as e:
            print(str(e))
        
    def buscar(self, query):
        """Função genérica para consulta de dados no 
            banco de dados

        Args:
            query (string): query SQL completa para 
                consultas no banco de dados
        Returns:
            list: lista com todos os dados lidos do 
                banco de dados
        """
        try:
            dados = self.session.execute(query)
            lista = self.fetchall(dados)
            return lista
        except Exception as e:
            print(str(e))
            
    def inserir(self, query):
        """Função genérica para inserção de dados no 
            banco de dados

        Args:
            query (string): query SQL completa para inserir 
                dados no banco de dados
        """
        try:
            self.session.execute(query)
        except Exception as e:
            print(str(e))
            
    def atualizar(self, query):
        """Função genérica para atualização de dados 
            no banco de dados

        Args:
            query (string): query SQL completa para 
                atualizar dados no banco de dados
        """
        try:
            self.session.execute(query)   
        except Exception as e:
            print(str(e))
              
    def deletar(self, query):
        """Função genérica para deleção de dados no 
            banco de dados

        Args:
            query (string): query SQL completa para 
                deletar dados no banco de dados
        """
        try:
            self.session.execute(query)  
        except Exception as e:
            print(str(e))
        