import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import os

DB_NAME = ""
DB_USER = ""
DB_PASS = ""
DB_HOST = ""
DB_PORT = ""
SAVE_FOLDER = ""
os.makedirs(SAVE_FOLDER, exist_ok=True)

#PSQL
conn = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public';")
tables = [row[0] for row in cursor.fetchall()]

for table in tables:
    cursor.execute(f"SELECT date, number FROM {table};")
    data = cursor.fetchall()
    
    if not data:
        continue
    
    df = pd.DataFrame(data, columns=["date", "number"])
    #year fix
    df["date"] = df["date"].apply(lambda x: datetime.strptime(str(x), "%d.%m.%Y").replace(year=2000))
    #week n
    df["week"] = df["date"].dt.isocalendar().week
    
    weekly_avg = df.groupby("week")["number"].median().reset_index()
    
    plt.figure(figsize=(12, 5))
    plt.bar(weekly_avg["week"], weekly_avg["number"], color="skyblue", label=table)
    plt.xlabel("Week of Year")
    plt.ylabel("Average Number")
    plt.title(f"Average Observations per Week for {table}")
    plt.xticks(range(1, 54))
    plt.legend()
    plt.tight_layout()
    save_path = os.path.join(SAVE_FOLDER, f"{table}_weekly.png")
    plt.savefig(save_path)
    plt.close()

cursor.close()
conn.close()