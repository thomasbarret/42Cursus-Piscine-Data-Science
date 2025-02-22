import psycopg2
import matplotlib.pyplot as plt
import numpy

def load_data(dbname, user, password, query):
    """
    Charge les données à partir d'une requête SQL fournie en argument.

    Args:
        dbname (str): Nom de la base de données.
        user (str): Nom d'utilisateur PostgreSQL.
        password (str): Mot de passe PostgreSQL.
        query (str): La requête SQL à exécuter.

    Returns:
        list: Liste de tuples contenant les résultats de la requête.
    """
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
    """
    Fonction principale pour charger les données, calculer les statistiques et générer le box plot.
    """
    try:
        dbname = "piscineds"
        user = "tbarret" 
        password = "mysecretpassword"

        query = """
            SELECT 
                event_type, price
            FROM 
                customers
            WHERE
                event_time >= '2023-02-01' AND event_time < '2023-03-01';

        """

        data = load_data(dbname, user, password, query)

        if data is None:
            print("Erreur lors du chargement des données.  Vérifiez les messages d'erreur précédents.")
            return

        print(data)

        prices = [i[1] for i in data if i[0] == "purchase"]

        if not prices:
            print("Aucune donnée de type 'purchase' trouvée.")
            return

        count = len(prices)
        mean = numpy.mean(prices)
        std = numpy.std(prices)
        min_price = numpy.min(prices)
        max_price = numpy.max(prices)
        quartiles = numpy.percentile(prices, [25, 50, 75])

        print(f"count   {count:.6f}")
        print(f"mean    {mean:.6f}")
        print(f"std     {std:.6f}")
        print(f"min     {min_price:.6f}")
        print(f"25%     {quartiles[0]:.6f}")
        print(f"50%     {quartiles[1]:.6f}")
        print(f"75%     {quartiles[2]:.6f}")
        print(f"max     {max_price:.6f}")


        plt.figure(figsize=(8, 6))  # Ajuste la taille de la figure pour une meilleure visualisation
        plt.boxplot(prices, vert=False, widths=0.5, patch_artist=True, boxprops=dict(facecolor="lightblue")) # Ajout de couleur
        plt.xlabel("Price (Altairian Dollars)") # Ajout d'une unité
        plt.title("Box Plot des prix d'achat") # Titre plus explicite
        plt.show()

        # Deuxième box plot (optionnel, sans les valeurs aberrantes)
        #plt.figure(figsize=(8, 6))
        #plt.boxplot(prices, vert=False, widths=0.5, showfliers=False, patch_artist=True, boxprops=dict(facecolor="lightgreen"))
        #plt.xlabel("Price (Altairian Dollars)")
        #plt.title("Box Plot des prix d'achat (sans valeurs aberrantes)")
        #plt.show()

    except Exception as error:
        print(f"{type(error).__name__} : {error}") # Affichage du type d'erreur

if __name__ == "__main__":
    main()
