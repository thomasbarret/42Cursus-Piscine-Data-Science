import psycopg2
from sqlalchemy import create_engine, text

def remove_duplicate_customers(username, password, dbname="piscineds", host="localhost", port="5432"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'customers' AND column_name = 'event_time';
            """))
            column_exists = result.fetchone()

            if not column_exists:
                print("Erreur : La colonne 'event_time' n'existe pas dans 'customers'.")
                return

            cleanup_query = text("""
                CREATE TABLE customers_tmp (LIKE customers);

                INSERT INTO customers_tmp (event_time, event_type, product_id, price, user_id, user_session)
                SELECT DISTINCT ON (date_trunc('minute', event_time), event_type, product_id, price, user_id, user_session)
                    event_time, event_type, product_id, price, user_id, user_session
                FROM customers;

                DROP TABLE customers;

                ALTER TABLE customers_tmp RENAME TO customers;
            """)

            connection.execute(cleanup_query)
            connection.commit()

            result = connection.execute(text("SELECT COUNT(*) FROM customers"))
            count = result.scalar()

            print(f"Doublons supprimés ! Nouveau nombre de lignes : {count}")
        
    except Exception as e:
        print(f"Erreur lors de la suppression des doublons : {e}")
        raise e

if __name__ == "__main__":
    remove_duplicate_customers("tbarret", "mysecretpassword")
