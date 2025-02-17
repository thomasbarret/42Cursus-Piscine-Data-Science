import psycopg2
from sqlalchemy import create_engine, text

def merge_with_items(username, password, dbname="piscineds", host="localhost", port="5432"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        
        with engine.connect() as conn:
            create_temp_table_query = text("""
                CREATE TABLE customers_left_joined_items (
                    event_time TIMESTAMP,
                    event_type VARCHAR(255),
                    product_id SERIAL,
                    price DECIMAL,
                    user_id SERIAL,
                    user_session VARCHAR(36),
                    items_category_id VARCHAR(255),
                    items_brand VARCHAR(255)
                );
            """)
            conn.execute(create_temp_table_query)
            conn.commit()
            
            insert_query = text("""
                INSERT INTO customers_left_joined_items
                SELECT customers.event_time, 
                       customers.event_type,
                       customers.product_id,
                       customers.price,
                       customers.user_id,
                       customers.user_session,
                       items.category_id AS item_category_id,
                       items.brand AS item_brand
                FROM customers
                LEFT JOIN items ON customers.product_id = items.product_id;
            """)
            conn.execute(insert_query)
            conn.commit()

            drop_table_query = text("DROP TABLE IF EXISTS customers;")
            conn.execute(drop_table_query)
            conn.commit()

            rename_table_query = text("ALTER TABLE customers_left_joined_items RENAME TO customers;")
            conn.execute(rename_table_query)
            conn.commit()

        print("✅ Fusion avec la table items réalisée avec succès!")

    except Exception as e:
        print(f"❌ Erreur lors de la fusion avec items: {e}")

if __name__ == "__main__":
    merge_with_items("tbarret", "mysecretpassword")
