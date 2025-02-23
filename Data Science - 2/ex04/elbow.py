import matplotlib
# matplotlib.use('gtk3agg')
import matplotlib.pyplot as plt
import psycopg2
import pandas as pd
from psycopg2 import Error
from sqlalchemy import create_engine
import signal
import sys
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def signal_handler(sig, frame):
    print("\nInterruption détectée. Fermeture du programme.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_purchase_data(username, password, dbname="piscineds"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        query = """
            SELECT user_id, COUNT(*) AS purchases
            FROM customers
            WHERE event_type = 'purchase'
            GROUP BY user_id
            HAVING COUNT(*) < 30
            ORDER BY purchases DESC;
        """
        
        df = pd.read_sql(query, engine)
        return df
    
    except (Exception, Error) as error:
        print("Erreur lors de la récupération des données d'achat:", error)
        return pd.DataFrame()

def plot_elbow_method(df):
    if df.empty:
        print("Aucune donnée à analyser.")
        return
    
    scaler = StandardScaler()
    X = scaler.fit_transform(df[["purchases"]])
    
    inertias = []
    k_values = range(1, 11)
    
    for k in k_values:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(X)
        inertias.append(kmeans.inertia_)

    plt.figure(figsize=(8, 6))
    plt.plot(k_values, inertias, color='#4c72b0')
    plt.xlabel("Nomber of clusters")
    plt.title("The Elbow Method")
    plt.grid()
    
    plt.show()

if __name__ == "__main__":
    try:
        df_purchases = get_purchase_data("tbarret", "mysecretpassword")
        print(df_purchases.head())
        plot_elbow_method(df_purchases)
    except KeyboardInterrupt:
        print("\nProgramme interrompu par l'utilisateur.")
        sys.exit(0)
