import os
import pandas as pd
from sqlalchemy import create_engine

def create_tables_from_folder(username, password, folder_path, dbname="piscineds"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_csv(file_path)
                
                df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
                
                table_name = filename.replace('.csv', '')
                
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                
                print(f"Table {table_name} créée avec succès")
                
    except Exception as error:
        print("Erreur lors de la création des tables:", error)

if __name__ == "__main__":
    create_tables_from_folder("tbarret", "mysecretpassword", "customer")