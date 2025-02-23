import psycopg2
import matplotlib.pyplot as plt
import numpy as np

def load_data(query):
    conn = psycopg2.connect(
        host="localhost",
        database="piscineds",
        user="tbarret",
        password="mysecretpassword"
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

def plot_frequency_histogram(data):
    frequency = [row[1] for row in data if row[1] < 30]
    
    plt.figure(figsize=(12, 6))
    plt.hist(frequency, bins=5, range=(0, 30), rwidth=1.0, color='#4c72b0', alpha=0.8, edgecolor="white")
    
    plt.title("Distribution du nombre de commandes par utilisateur")
    plt.xlabel("frequency")
    plt.ylabel("customers")
    
    plt.yticks(range(0, 70000, 10000))
    plt.xticks([0, 10, 20, 30])
    
    plt.ylim(0, 60000)
    plt.xlim(0, 30)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def plot_value_histogram(data):
    values = [row[1] for row in data if row[1] < 200]
    
    plt.figure(figsize=(12, 6))
    plt.hist(values, bins=5, range=(0, 200), rwidth=1.0, color='#55a868', alpha=0.8, edgecolor="white")
    
    plt.title("Distribution des dépenses totales par utilisateur")
    plt.xlabel("monetary value in ₳")
    plt.ylabel("customers")
    
    plt.yticks(range(0, 45000, 5000))
    plt.xticks([0, 50, 100, 150, 200])
    
    plt.ylim(0, 40000)
    plt.xlim(0, 200)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def main():
    try:
        query_frequency = """
        SELECT user_id, COUNT(*)
        FROM customers
        WHERE event_type = 'purchase'
        GROUP BY user_id;
        """

        query_value = """
        SELECT user_id, SUM(price)
        FROM customers
        WHERE event_type = 'purchase'
        GROUP BY user_id
        HAVING SUM(price) < 200;
        """

        data_frequency = load_data(query_frequency)
        data_value = load_data(query_value)
        
        plot_frequency_histogram(data_frequency)
        plot_value_histogram(data_value)
        
    except Exception as error:
        print(f"{type(error).__name__} : {error}")

if __name__ == "__main__":
    main()
