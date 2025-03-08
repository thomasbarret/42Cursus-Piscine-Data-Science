import matplotlib
# matplotlib.use('gtk3agg')
import matplotlib.pyplot as plt
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def get_element_counts_per_day(username, password, dbname="piscineds"):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=dbname,
            user=username,
            password=password
        )
        query = """
            SELECT 
                event_time, 
                event_type
            FROM 
                customers
            ORDER BY 
                event_time
        """
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        conn.close()

        purchase_counts = {}
        for event_time, event_type in results:
            if event_type == 'purchase':
                date = datetime(event_time.year, event_time.month, event_time.day)
                date_str = date.strftime('%Y-%m-%d')
                if date_str not in purchase_counts:
                    purchase_counts[date_str] = 0
                purchase_counts[date_str] += 1

        dates = list(purchase_counts.keys())
        counts = list(purchase_counts.values())
        return pd.DataFrame({'day': dates, 'count': counts})
    
    except Exception as error:
        print("Erreur lors de la récupération des événements:", error)
        return pd.DataFrame()

def get_sales_data(username, password, dbname="piscineds"):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=dbname,
            user=username,
            password=password
        )
        query = """
            SELECT 
                event_time, 
                event_type, 
                price
            FROM 
                customers
        """
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        conn.close()

        monthly_sales = {}
        for event_time, event_type, price in results:
            if event_type == 'purchase':
                date = datetime(event_time.year, event_time.month, 1)
                date_str = date.strftime('%Y-%m')
                if date_str not in monthly_sales:
                    monthly_sales[date_str] = 0
                monthly_sales[date_str] += price

        months = list(monthly_sales.keys())
        sales = list(monthly_sales.values())
        return pd.DataFrame({'month': months, 'total_price': sales})
    
    except Exception as error:
        print("Erreur lors de la récupération des ventes:", error)
        return pd.DataFrame()

def get_avg_spend_data(username, password, dbname="piscineds"):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database=dbname,
            user=username,
            password=password
        )
        query = """
            SELECT 
                user_id, 
                event_time, 
                event_type, 
                price
            FROM 
                customers
            ORDER BY 
                event_time
        """
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()
        cur.close()
        conn.close()

        daily_sales = {}
        unique_customers = {}
        
        for user_id, event_time, event_type, price in results:
            if event_type == 'purchase':
                date_str = event_time.strftime('%Y-%m-%d')
                
                if date_str not in daily_sales:
                    daily_sales[date_str] = 0
                daily_sales[date_str] += price

                if date_str not in unique_customers:
                    unique_customers[date_str] = set()
                unique_customers[date_str].add(user_id)

        dates = list(daily_sales.keys())
        average_per_customers = [daily_sales[date] / len(unique_customers[date]) for date in dates]
        return pd.DataFrame({'day': dates, 'average_spend': average_per_customers})
    
    except Exception as error:
        print("Erreur lors de la récupération des dépenses moyennes:", error)
        return pd.DataFrame()

def plot_event_counts(df):
    dates = df['day']
    counts = df['count']
    
    plt.plot(dates, counts)
    plt.ylabel("Number of customers")
    
    ticks = [0, len(dates) // 5, 2 * len(dates) // 5, 3 * len(dates) // 5, 4 * len(dates) // 5]
    labels = ["Oct", "Nov", "Dec", "Jan", "Feb"]
    
    plt.xticks(ticks, labels)
    
    plt.xlim(dates.iloc[0], dates.iloc[-1])
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def plot_sales_chart(df):
    df['month'] = pd.to_datetime(df['month'])
    df['month_str'] = df['month'].dt.strftime('%b')
    
    plt.figure(figsize=(10, 6))
    plt.bar(df["month_str"], df["total_price"], color='#B6C5D8')
    plt.xlabel("months")
    plt.ylabel("total sales in million of ₳")
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

def plot_avg_spend_per_customer(df):
    dates = df['day']
    
    plt.figure(figsize=(10, 6))
    plt.plot(dates, df['average_spend'], color="#B0C4DE", alpha=0.6)
    plt.fill_between(dates, df['average_spend'], color="#B0C4DE", alpha=0.3)
    
    plt.ylabel("average spend/customers in ₳")
    
    ticks = pd.date_range(start='2022-10-01', end='2023-02-28', freq='MS').strftime("%Y-%m-%d").tolist()
    labels = ["Oct", "Nov", "Dec", "Jan", "Feb"]
    
    plt.xticks(ticks=ticks, labels=labels)
    plt.xlim(dates.iloc[0], dates.iloc[-1])
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    username = "tbarret"
    password = "mysecretpassword"
    
    df_counts = get_element_counts_per_day(username, password)
    df_sales = get_sales_data(username, password)
    df_avg_spend = get_avg_spend_data(username, password)
    
    if not df_counts.empty:
        plot_event_counts(df_counts)
    if not df_sales.empty:
        plot_sales_chart(df_sales)
    if not df_avg_spend.empty:
        plot_avg_spend_per_customer(df_avg_spend)
