import pandas as pd
from sqlalchemy import create_engine

def create_items_table(username, password, csv_file, dbname="piscineds"):
    try:
        df = pd.read_csv(csv_file)
        
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        
        df.to_sql('items', engine, if_exists='replace', index=False)
        
        print("Table items créée avec succès")
        
    except Exception as error:
        print("Erreur lors de la création de la table items:", error)

if __name__ == "__main__":
    create_items_table("tbarret", "mysecretpassword", "items/items.csv")
