import psycopg2
import matplotlib.pyplot as plt
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA

def load_data(query):
    """Charge les données depuis PostgreSQL en réutilisant la connexion du fichier Building.py"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="piscineds",
            user="tbarret",
            password="mysecretpassword"
        )
        cur = conn.cursor()
        cur.execute(query)
        data = cur.fetchall()
        return np.array(data)
    except Exception as e:
        print(f"Database error: {e}")
        return None
    finally:
        cur.close()
        conn.close()

def prepare_features(X):
    """Normalisation des données avec gestion des NaN"""
    scaler = StandardScaler()
    return scaler.fit_transform(np.nan_to_num(X)), scaler

def plot_clusters(X, labels, centroids, title):
    """Visualisation PCA 2D inspirée des exemples GitHub"""
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X)
    
    plt.figure(figsize=(12, 8))
    scatter = plt.scatter(X_pca[:, 0], X_pca[:, 1], c=labels, cmap='viridis', alpha=0.6)
    plt.scatter(pca.transform(centroids)[:, 0], pca.transform(centroids)[:, 1], 
                marker='X', s=200, c='red', label='Centroids')
    plt.title(f"{title}\nPCA Projection")
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.colorbar(scatter)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def plot_cluster_profile(centroids, features, scaler):
    """Profil des clusters inspiré des visualisations du sujet"""
    plt.figure(figsize=(14, 7))
    
    # Dénormalisation des centroïdes
    centroids_denorm = scaler.inverse_transform(centroids)
    
    # Création du radar chart
    angles = np.linspace(0, 2 * np.pi, len(features), endpoint=False).tolist()
    angles += angles[:1]
    
    for i, center in enumerate(centroids_denorm):
        values = center.tolist()
        values += values[:1]
        
        ax = plt.subplot(1, centroids.shape[0], i+1, polar=True)
        ax.plot(angles, values, color='tab:blue', linewidth=2, linestyle='solid')
        ax.fill(angles, values, color='tab:blue', alpha=0.4)
        ax.set_theta_offset(np.pi / 2)
        ax.set_theta_direction(-1)
        ax.set_thetagrids(np.degrees(angles[:-1]), features)
        ax.set_title(f'Cluster {i}', pad=20)
    
    plt.suptitle("Profil des clusters normalisés", y=1.08)
    plt.tight_layout()
    plt.show()

def main():
    # Requête SQL unifiée avec gestion des schémas
    query = """
    SELECT 
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_session END) AS purchases,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_spent,
        EXTRACT(DAY FROM CURRENT_DATE - MAX(event_time)) AS days_inactive,
        COUNT(DISTINCT user_session) AS total_sessions
    FROM (
        SELECT event_time, event_type, product_id, price::numeric, user_id, user_session
        FROM customers
        UNION ALL
        SELECT event_time, event_type, product_id, price, user_id, user_session 
        FROM data_2022_oct
        UNION ALL 
        SELECT event_time, event_type, product_id, price, user_id, user_session FROM data_2022_nov
        UNION ALL 
        SELECT event_time, event_type, product_id, price, user_id, user_session FROM data_2022_dec
        UNION ALL 
        SELECT event_time, event_type, product_id, price, user_id, user_session FROM data_2023_jan 
        UNION ALL 
        SELECT event_time, event_type, product_id, price, user_id, user_session FROM data_2023_feb
    ) AS unified_data
    GROUP BY user_id
    HAVING SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) > 0;
    """
    
    # Pipeline de traitement
    raw_data = load_data(query)
    if raw_data is None: return
    
    X, scaler = prepare_features(raw_data)
    
    # Clustering K-Means avec 4 groupes
    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X)
    
    # Visualisations
    features = ['Achats', 'Dépenses', 'Inactivité', 'Sessions']
    plot_clusters(X, labels, kmeans.cluster_centers_, "Segmentation client")
    plot_cluster_profile(kmeans.cluster_centers_, features, scaler)
    
    # Interprétation des clusters
    cluster_profile = {
        0: 'Nouveaux (Activité récente, faible dépense)',
        1: 'Inactifs (Anciens clients)',
        2: 'Silver (Fidélité moyenne)',
        3: 'Gold (Clients VIP)'
    }
    print("\nInterprétation des clusters :")
    for cluster_id, description in cluster_profile.items():
        print(f"- Cluster {cluster_id} : {description}")

if __name__ == "__main__":
    main()
