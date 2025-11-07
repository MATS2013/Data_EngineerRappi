import sqlite3
import pandas as pd
class local_cnx:
    def __init__(self, db_path:str):
        self.db_path = db_path
        self.cnxn = None

    def sql_connection(self):
        """
        Se conecta con la base de datos SQLite
        """
        try:
            # Si no existe la crea
            self.cnxn = sqlite3.connect(self.db_path)
        except Exception as e:
            print(f'Error al conectar con la base de datos SQLite: {e}')

    def get_sql_table(self, query):
        """
        Ejecuta la consulta SQL para hacer CUD
        """
        if self.cnxn is None:
            self.sql_connection()
        if self.cnxn:
            try:
                df = pd.read_sql(query, self.cnxn)
                self.cnxn.commit()
                return df
            except Exception as e:
                print(f'Error ejecutando la consulta SQL: {e}')

    def bulk_insert_sql(self, query, df:pd.DataFrame):
        """
        Inserta multiples dilas en tabla de SQL Server a partir de un dataframe
        """
        if self.cnxn is None:
            self.sql_connection()
        if self.cnxn:
            try:
                cursor = self.cnxn.cursor()
                data_bulk = [tuple(row) for row in df.itertuples(index=False, name=None)]
                cursor.executemany(query, data_bulk)
                self.cnxn.commit()
            except Exception as e:
                print(f"\nError en la inserción masiva: {e}")

    def set_sql_table(self, query:str):
        #Segun lo consultado cursor funcionara como el
        #intermediario entre Python y la base de datos
        #donde cursor ejecutara el query
        if self.cnxn is None:
            self.sql_connection()
        if self.cnxn:
            cursor = self.cnxn.cursor()
            #Ejecuta QUERY
            cursor.execute(query)
            self.cnxn.commit()

    def get_connection(self):
        """ Devuelve la conexión establecida (SQL o SAP). """
        return self.cnxn

    def close_connection(self):
        """ Cierra la conexión SQL. """
        if self.cnxn:
            self.cnxn.commit()
            self.cnxn.close()
            self.cnxn = None