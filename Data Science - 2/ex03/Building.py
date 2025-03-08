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

def plot_histogram(data, title, xlabel, ylabel, bins, range, xticks, yticks, ylim, xlim, color):
    plt.figure(figsize=(12, 6))
    plt.hist(data, bins=bins, range=range, rwidth=1.0, color=color, alpha=0.8, edgecolor="white")
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    
    plt.yticks(yticks)
    plt.xticks(xticks)
    
    plt.ylim(ylim)
    plt.xlim(xlim)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def plot_frequency_histogram(data):
    frequency = [row[1] for row in data if row[1] < 40]
    plot_histogram(
        frequency,
        title="frequency",
        xlabel="frequency",
        ylabel="customers",
        bins=5,
        range=(0, 40),
        xticks=range(0, 41, 10),
        yticks=range(0, 70000, 10000),
        ylim=(0, 65000),
        xlim=(0, 40),
        color='#4c72b0'
    )

def plot_value_histogram(data):
    values = [row[1] for row in data if row[1] < 250]
    plot_histogram(
        values,
        title="monetary value in ₳",
        xlabel="monetary value in ₳",
        ylabel="customers",
        bins=5,
        range=(0, 250),
        xticks=range(0, 251, 50),
        yticks=range(0, 45000, 5000),
        ylim=(0, 45000),
        xlim=(0, 250),
        color='#55a868'
    )

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
        HAVING SUM(price) < 225;
        """

        data_frequency = load_data(query_frequency)
        data_value = load_data(query_value)
        
        plot_frequency_histogram(data_frequency)
        plot_value_histogram(data_value)
        
    except Exception as error:
        print(f"{type(error).__name__} : {error}")

if __name__ == "__main__":
    main()
