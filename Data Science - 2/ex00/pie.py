import matplotlib
# matplotlib.use('gtk3agg')
import matplotlib.pyplot as plt
import psycopg2
import pandas as pd
from psycopg2 import Error
from sqlalchemy import create_engine
import signal
import sys

def signal_handler(sig, frame):
    print("\nInterruption détectée. Fermeture du programme.")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def get_event_type_counts(username, password, dbname="piscineds"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        query = """
            SELECT event_type, COUNT(*) as count
            FROM customers
            GROUP BY event_type
        """
        
        df = pd.read_sql(query, engine)
        event_counts = df.set_index("event_type")["count"].to_dict()
        
        return event_counts
    
    except (Exception, Error) as error:
        print("Erreur lors de la récupération des événements:", error)
        return {}

def plot_event_counts(event_counts):
    plt.figure(figsize=(8, 6))
    plt.pie(
        event_counts.values(), 
        labels=event_counts.keys(), 
        autopct='%1.1f%%', 
        colors=['#4c72b0', '#dd8452', '#55a868', '#c44e52']
    )
    plt.title("Répartition des types d'événements")
    
    plt.show() 

if __name__ == "__main__":
    try:
        counts = get_event_type_counts("tbarret", "mysecretpassword")
        print(counts)
        plot_event_counts(counts)
    except KeyboardInterrupt:
        print("\nProgramme interrompu par l'utilisateur.")
        sys.exit(0)
