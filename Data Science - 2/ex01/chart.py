import matplotlib
# matplotlib.use('gtk3agg')
import matplotlib.pyplot as plt
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def get_element_counts_per_day(username, password, dbname="piscineds"):
    try:
        engine = create_engine(f'postgresql://{username}:{password}@localhost:5432/{dbname}')
        query = """
            SELECT 
                DATE(event_time) as day,
                COUNT(*) as count,
                SUM(price) as total_price
            FROM 
                customers
            WHERE 
                event_time >= '2022-10-01' AND event_time < '2023-03-01'
            GROUP BY 
                day
            ORDER BY 
                day
        """
        df = pd.read_sql(query, engine)
        return df
    
    except Exception as error:
        print("Erreur lors de la récupération des événements:", error)
        return pd.DataFrame()

def plot_event_counts(df):
    dates = df['day']
    counts = df['count']
    
    plt.plot(dates, counts)
    plt.ylabel("Number of customers")
    
    ticks = pd.date_range(start='2022-10-01', end='2023-02-28', freq='MS').strftime("%Y-%m-%d").tolist()
    labels = ["Oct", "Nov", "Dec", "Jan", "Feb"]
    
    plt.xticks(ticks=ticks, labels=labels)
    
    plt.xlim(dates.iloc[0], dates.iloc[-1])
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_sales_chart(df):
    df['day'] = pd.to_datetime(df['day'])  

    df['month'] = df['day'].dt.strftime('%b')  

    df_monthly = df.groupby('month', sort=False)['total_price'].sum().reset_index()
    
    plt.figure(figsize=(10, 6))
    plt.bar(df_monthly["month"], df_monthly["total_price"], color='#B6C5D8')
    plt.xlabel("months")
    plt.ylabel("total sales in million of ₳")
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

def plot_avg_spend_per_customer(df):
    df['average_spend'] = df['total_price'] / df['count']
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
    
    df = get_element_counts_per_day(username, password)
    if not df.empty:
        plot_event_counts(df)
        plot_sales_chart(df)
        plot_avg_spend_per_customer(df)
