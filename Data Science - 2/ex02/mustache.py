import psycopg2
import matplotlib.pyplot as plt
import numpy
from decimal import Decimal

def load_data(dbname, user, password, query):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=dbname,
            user=user,
            password=password
        )
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()

        cur.close()
        conn.commit()
        conn.close()

        return data

    except psycopg2.Error as e:
        print(f"Erreur de base de données : {e}")
        return None
    except Exception as e:
        print(f"Erreur inattendue : {e}")
        return None

def main():
    try:
        dbname = "piscineds"
        user = "tbarret" 
        password = "mysecretpassword"

        query = """
            SELECT 
                event_type, price
            FROM 
                customers
        """

        data = load_data(dbname, user, password, query)

        if data is None:
            print("Erreur lors du chargement des données. Vérifiez les messages d'erreur précédents.")
            return

        prices = [float(i[1]) for i in data if i[0] == "purchase" and i[1] is not None]

        if not prices:
            print("Aucune donnée de type 'purchase' trouvée.")
            return

        count = len(prices)
        mean = numpy.mean(prices)
        std = numpy.std(prices)
        min_price = numpy.min(prices)
        max_price = numpy.max(prices)
        quartiles = numpy.percentile(prices, [25, 50, 75])

        print(f"count   {count}")
        print(f"mean    {mean:.6f}")
        print(f"std     {std:.6f}")
        print(f"min     {min_price:.6f}")
        print(f"25%     {quartiles[0]:.6f}")
        print(f"50%     {quartiles[1]:.6f}")
        print(f"75%     {quartiles[2]:.6f}")
        print(f"max     {max_price:.6f}")

        plt.figure(figsize=(8, 6))
        plt.boxplot(prices, vert=False, widths=0.5, patch_artist=True, 
                   boxprops=dict(facecolor="lightblue"))
        plt.xlabel("price")
        plt.show()
     
        plt.boxplot(prices, vert=False, widths=0.5, notch=True, showfliers=False, patch_artist=True, boxprops=dict(facecolor="lightgreen"))
        plt.xlabel("price")
        plt.show()

        basket_query = """
            SELECT user_id, AVG(price) AS avg_cart_price
            FROM customers
            WHERE event_type = 'cart'
            GROUP BY user_id
            HAVING AVG(price) BETWEEN 26 and 43
        """

        basket_data = load_data(dbname, user, password, basket_query)

        if basket_data is None:
            print("Erreur lors du chargement des données de panier. Vérifiez les messages d'erreur précédents.")
            return

        average_basket_price = [float(row[1]) for row in basket_data]

        plt.figure(figsize=(8, 6))
        plt.boxplot(average_basket_price, vert=False, widths=0.5, notch=True, patch_artist=True, boxprops=dict(facecolor="lightblue"))
        plt.xlabel("average basket price")
        plt.show()

    except Exception as error:
        print(f"{type(error).__name__} : {error}")

if __name__ == "__main__":
    main()
