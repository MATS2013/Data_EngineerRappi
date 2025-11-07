
from src.connection.sql_engine import local_cnx
from src.constants.queries import drop_query
import pandas as pd, os
# Clase para crear la base de datos local a partir de los CSVs de referencia
class createDataBase():
    def __init__(self):
        # Rutas de los archivos y base de datos
        current_path = "/opt/airflow"
        self.accounts_path = os.path.join(current_path,"data","reference","accounts.csv")
        self.journal_entries_path = os.path.join(current_path,"data","reference","journal_entries.csv")
        self.local_db_path = os.path.join(current_path,"data","reference","local.db")
        self.local_db = local_cnx(self.local_db_path)
        self.local_db.sql_connection()
        self._readData()
    def _readData(self):        
        cnxn = self.local_db.cnxn
        # Asegura que las tablas esten limpias 
        self.local_db.set_sql_table(drop_query.replace("_TablePlaceHolder_", "accounts"))
        self.local_db.set_sql_table(drop_query.replace("_TablePlaceHolder_", "journal_entries"))
        # Carga los datos desde los CSV a un dataframe
        df_accounts = pd.read_csv(self.accounts_path)
        df_journal_entries = pd.read_csv(self.journal_entries_path)
        # Inserta los datos en las tablas correspondientes
        df_accounts.to_sql("accounts", cnxn, if_exists='replace', index=False)
        df_journal_entries.to_sql("journal_entries", cnxn, if_exists='replace', index=False)
        self.local_db.close_connection()
        print("Base de datos creada correctamente.")