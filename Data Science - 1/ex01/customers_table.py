import psycopg2
from sqlalchemy import create_engine, text

def merge_customer_tables(username, password, dbname="piscineds", host="localhost", port="5432"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT table_name a
                FROM information_schema.tables 
                WHERE table_name LIKE 'data_202%'
            """))
            tables = [row[0] for row in result]
            
            if not tables:
                print("Aucune table source trouvée.")
                return
            
            union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in tables])
            create_table_query = f"""
                DROP TABLE IF EXISTS customers;
                CREATE TABLE customers AS 
                {union_query}
            """
            
            connection.execute(text(create_table_query))
            connection.commit()
            
            result = connection.execute(text("SELECT COUNT(*) FROM customers"))
            count = result.scalar()
            
            print(f"Table customers créée avec succès! Nombre de lignes: {count}")
        
    except Exception as e:
        print(f"Erreur lors de la création de la table customers: {e}")
        raise e

if __name__ == "__main__":
    merge_customer_tables("tbarret", "mysecretpassword")