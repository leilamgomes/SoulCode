import mysql.connector

class Interface_db_mysql():
    
    usuario, senha, host, banco = "","","",""

    def __init__(self, usuario, senha, host, banco):
        """Construtor da classe Interface_db_mysql

        Args:
            usuario (string): usuario do banco
            senha (string): senha de acesso ao banco
            host (string): ip de acesso ao banco
            banco (string): nome do banco
        """
        try:
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.banco = banco
        except Exception as e:
            print(str(e))
    
    def conectar(self):
        """Função genérica para conectar ao banco

        Returns:
            con : conector mysql
            cursor : cursor para leitura do banco
        """
        try:
            con = mysql.connector.connect(user = self.usuario, password = self.senha, 
                                          host = self.host, database=self.banco)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))

    def desconectar(self,con,cursor):
        """Função genérica para desconectar do banco

        Args:
            con : conector mysql
            cursor : cursor para leitura do banco
        """
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e))

    def buscar(self, query):
        """Função genérica para uma consulta no banco de dados

        Args:
            query (string): Query de busca
        Returns:
            cursor.fetchall(): retorna tudo que for encontrado
                pelo cursor
        """
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
            return cursor.fetchall() #Retorna tudo o que for encontrado pelo cursor na busca realizada no banco de dados
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
     
    def inserir(self, query):
        """Função genérica para inserir um dado no 
            banco de dados

        Args:
            query (string): Query de inserção 
        Returns:
            cursor.fetchall(): retorna tudo que for 
                encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
            
    def atualizar(self, query):
        """Função genérica para alterar um dado no 
            banco de dados

        Args:
            query (string): query de atualização
        Returns:
            cursor.fetchall(): retorna tudo que for 
                encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)

    def deletar(self, query):
        """Função genérica para um delete de algum dado 
            no banco de dados

        Args:
            query (string): query de inserção 
        Returns:
            cursor.fetchall(): retorna tudo que for 
                encontrado pelo cursor
        """
        try:
            con, cursor = self.conectar()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(str(e))
        finally:
            self.desconectar(con,cursor)
    