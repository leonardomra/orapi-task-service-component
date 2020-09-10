import mysql.connector
from mysql.connector import errorcode, Error

class MySQLHandler():
    
    def __init__(self, user: str=None, password: str=None, host: str=None, database: str=None, raise_on_warnings: bool=True):
        self.config = {
            'user': user,
            'password': password,
            'host': host,
            'database': database,
            'raise_on_warnings': True
        }
        self.cnx = None
        self.cursor = None

    def dbconnect(self):   
        try:
            self.cnx = mysql.connector.connect(**self.config)
            self.cursor = self.cnx.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            print('DB Connected!')

    def dbCloseConnection(self):
        self.cursor.close()
        self.cnx.close()

    def info(self):
        self.dbconnect()
        self.dbCloseConnection()
        print(self.config)

    def add(self, schema, data):
        self.dbconnect()
        self.cursor.execute(schema, data)
        self.cnx.commit()
        self.dbCloseConnection()

    def get(self, query, params=None):
        self.dbconnect()
        self.cursor.execute(query, params)
        records = self.cursor.fetchall()
        self.dbCloseConnection()
        return records

    def update(self, query, params=None):
        self.dbconnect()
        self.cursor.execute(query, params)
        self.cnx.commit()
        self.dbCloseConnection()

    def delete(self, table, id, user):
        query = "DELETE FROM " + table + " WHERE id=%s and user=%s"
        params = (id, user)
        response = False
        try:
            self.dbconnect()
            self.cursor.execute(query, params)
            self.cnx.commit()
            response = True
        except Error as error:
            print(error, flush=True)
        finally:
            self.dbCloseConnection()
        return response