import matplotlib.pyplot as plt
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import seaborn as sns
from sqlalchemy import create_engine
import os

def get_rfm_data(username, password, dbname="piscineds"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        query = """
            SELECT 
                user_id,
                CURRENT_DATE - MAX(event_time::DATE) AS recency,
                COUNT(*) AS frequency,
                SUM(price)::FLOAT AS monetary
            FROM customers
            WHERE event_type = 'purchase'
            GROUP BY user_id;
        """
        return pd.read_sql(query, engine)
    
    except Exception as error:
        print("Erreur lors de la récupération RFM :", error)
        return pd.DataFrame()

def plot_clusters(df, labels):
    plt.figure(figsize=(12, 5))
    
    # Projection PCA
    pca = PCA(n_components=2)
    principal_components = pca.fit_transform(df)
    
    plt.subplot(121)
    sns.scatterplot(
        x=principal_components[:,0], 
        y=principal_components[:,1],
        hue=labels,
        palette="viridis",
        alpha=0.8
    )
    plt.title("Projection des clusters en 2D")
    
    # Profil des clusters
    plt.subplot(122)
    cluster_profile = df.groupby(labels).mean().reset_index()
    cluster_profile_melted = cluster_profile.melt(id_vars="cluster")
    
    sns.barplot(
        x="cluster",
        y="value",
        hue="variable",
        data=cluster_profile_melted,
        palette="coolwarm"
    )
    plt.title("Profil moyen des clusters")
    plt.tight_layout()
    plt.show()

def main_clustering(df):
    if df.empty:
        print("Données RFM non disponibles")
        return df
    
    features = df[["recency", "frequency", "monetary"]]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(features)
    
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    
    # Attribution des libellés basée sur les patterns
    df['cluster'] = clusters
    cluster_stats = df.groupby('cluster')[['recency', 'frequency', 'monetary']].mean()
    
    # Mapping dynamique selon les stats
    labels_map = {}
    for idx, stats in cluster_stats.iterrows():
        if stats['recency'] < 30:
            labels_map[idx] = 'Nouveaux'
        elif stats['frequency'] > 10:
            labels_map[idx] = 'Platine'
        else:
            labels_map[idx] = 'Inactifs'
    
    df['cluster_label'] = df['cluster'].map(labels_map)
    
    plot_clusters(pd.DataFrame(X_scaled, columns=features.columns), clusters)
    return df

if __name__ == "__main__":
    try:
        df_rfm = get_rfm_data("tbarret", "mysecretpassword")
        if not df_rfm.empty:
            print("Exemple de données RFM :\n", df_rfm.head())
            clustered_df = main_clustering(df_rfm)
            
            # Création du dossier si inexistant
            os.makedirs("ez05", exist_ok=True)
            
            if 'cluster_label' in clustered_df.columns:
                clustered_df[['user_id', 'cluster_label']].to_csv("ez05/Clustering.csv", index=False)
                print("Fichier sauvegardé avec succès.")
            else:
                print("Aucun cluster généré.")
        else:
            print("Aucune donnée récupérée depuis la base.")
            
    except Exception as e:
        print("Erreur dans le flux principal :", e)