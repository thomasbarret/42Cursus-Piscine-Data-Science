import psycopg2
from sqlalchemy import create_engine, text

def merge_with_items(username, password, dbname="piscineds", host="localhost", port="5432"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        
        with engine.connect() as conn:
            with conn.begin():
                create_table_query = text("""
                    CREATE TABLE IF NOT EXISTS customers_left_joined_items (
                        event_time TIMESTAMP,
                        event_type VARCHAR(255),
                        product_id INT,
                        price DECIMAL,
                        user_id INT,
                        user_session VARCHAR(36),
                        items_category_id VARCHAR(255),
                        items_brand VARCHAR(255)
                    );
                """)
                conn.execute(create_table_query)
                
                insert_query = text("""
                    INSERT INTO customers_left_joined_items
                    SELECT DISTINCT 
                           customers.event_time, 
                           customers.event_type,
                           customers.product_id,
                           customers.price,
                           customers.user_id,
                           customers.user_session,
                           CAST(items.category_id AS BIGINT) AS items_category_id,
                           items.brand AS items_brand
                    FROM customers
                    LEFT JOIN items ON customers.product_id = items.product_id;
                """)
                conn.execute(insert_query)

                count_before = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
                count_after = conn.execute(text("SELECT COUNT(*) FROM customers_left_joined_items")).scalar()
                
                print(f"Nombre de lignes avant fusion : {count_before}")
                print(f"Nombre de lignes après fusion : {count_after}")

                drop_table_query = text("DROP TABLE IF EXISTS customers;")
                conn.execute(drop_table_query)

                rename_table_query = text("ALTER TABLE customers_left_joined_items RENAME TO customers;")
                conn.execute(rename_table_query)

        print("✅ Fusion avec la table items réalisée avec succès, sans duplications!")

    except Exception as e:
        print(f"❌ Erreur lors de la fusion avec items: {e}")

if __name__ == "__main__":
    merge_with_items("tbarret", "mysecretpassword")