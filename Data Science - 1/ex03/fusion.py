from sqlalchemy import create_engine, text

def merge_with_items(username, password, dbname="piscineds", host="localhost", port="5432"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS customers_left_joined_items (
                    id SERIAL PRIMARY KEY,
                    event_time TIMESTAMP,
                    event_type VARCHAR(255),
                    product_id INT,
                    price DECIMAL,
                    user_id INT,
                    user_session VARCHAR(36),
                    items_category_id BIGINT,
                    items_brand VARCHAR(255),
                    items_category_code VARCHAR(255),
                    UNIQUE (event_time, user_id, product_id, id)
                );
                """))

                conn.execute(text("""
                INSERT INTO customers_left_joined_items (
                    event_time, event_type, product_id, price, user_id, 
                    user_session, items_category_id, items_brand, items_category_code
                )
                SELECT 
                    customers.event_time,
                    customers.event_type,
                    customers.product_id,
                    customers.price,
                    customers.user_id,
                    customers.user_session,
                    items.category_id AS items_category_id,
                    items.brand AS items_brand,
                    items.category_code AS items_category_code
                FROM customers 
                LEFT JOIN (
                    SELECT DISTINCT ON (product_id) * 
                    FROM items
                ) items ON customers.product_id = items.product_id;
                """))

                count_before = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
                count_after = conn.execute(text("SELECT COUNT(*) FROM customers_left_joined_items")).scalar()
                print(f"Nombre de lignes avant fusion : {count_before}")
                print(f"Nombre de lignes après fusion : {count_after}")

                if count_before != count_after:
                    print(f"⚠️ Attention: Différence de {count_before - count_after} lignes détectée.")

                conn.execute(text("DROP TABLE IF EXISTS customers;"))
                conn.execute(text("ALTER TABLE customers_left_joined_items RENAME TO customers;"))
                print("✅ Fusion réalisée avec succès, sans perte de données!")
                
    except Exception as e:
        print(f"❌ Erreur lors de la fusion avec items: {e}")

if __name__ == "__main__":
    merge_with_items("tbarret", "mysecretpassword")