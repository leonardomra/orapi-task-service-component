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
        _cnx = None
        _cursor = None
        try:
            #self.cnx = mysql.connector.connect(**self.config)
            #self.cursor = self.cnx.cursor()
            _cnx = mysql.connector.connect(**self.config)
            _cursor = _cnx.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            print('DB Connected!')
        return { 'cnx': _cnx, 'cursor': _cursor }

    def dbCloseConnection(self, db):
        db['cursor'].close()
        db['cnx'].close()

    def info(self):
        db = self.dbconnect()
        self.dbCloseConnection(db)
        print(self.config)

    def add(self, query, params=None):
        db = self.dbconnect()
        db['cursor'].execute(query, params)
        db['cnx'].commit()
        self.dbCloseConnection(db)

    def get(self, query, params=None):
        db = self.dbconnect()
        db['cursor'].execute(query, params)
        records = db['cursor'].fetchall()
        self.dbCloseConnection(db)
        return records

    def update(self, query, params=None):
        db = self.dbconnect()
        db['cursor'].execute(query, params)
        db['cnx'].commit()
        self.dbCloseConnection(db)

    def delete(self, table, id, user):
        query = "DELETE FROM " + table + " WHERE id=%s and user=%s"
        params = (id, user)
        response = False
        db = None
        try:
            db = self.dbconnect()
            db['cursor'].execute(query, params)
            db['cnx'].commit()
            response = True
        except Error as error:
            print(error, flush=True)
        finally:
            self.dbCloseConnection(db)
        return response