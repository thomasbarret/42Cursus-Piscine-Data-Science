import pandas as pd
import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine

def create_customer_table(username, password, csv_file, dbname="piscineds"):
    try:
        df = pd.read_csv(csv_file)
        
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
        
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        
        table_name = csv_file.split('/')[-1].replace('.csv', '')
        
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"Table {table_name} créée avec succès")
        
    except (Exception, Error) as error:
        print("Erreur lors de la création de la table:", error)

if __name__ == "__main__":
    create_customer_table("tbarret", "mysecretpassword", "data_2022_oct.csv")