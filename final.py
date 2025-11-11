from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.window import WindowTypes
from selenium.webdriver.firefox.service import Service
import time
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import psycopg2
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

load_dotenv("me.env")
ATLAS_USERNAME = os.getenv('ATLAS_USERNAME')
ATLAS_PASSWORD = os.getenv('ATLAS_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')


def create_species_table(cursor, species_name):
    table_name = species_name.replace(' ', '_').lower()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        tetrada_m VARCHAR(255),
        code VARCHAR(255),
        date VARCHAR(255),
        number INT
    );"""
    cursor.execute(create_table_query)


def insert_data_into_table(cursor, table_name, tetrada_m, code, date, number):
    insert_query = f'INSERT INTO "{table_name}" (tetrada_m, code, date, number) VALUES (%s, %s, %s, %s)'
    cursor.execute(insert_query, (tetrada_m, code, date, number))


def parse(podatki):
    soup = BeautifulSoup(podatki, 'html.parser')
    table = soup.find('table')
    data = []
    if table:
        for row in table.find_all('tr')[1:]:
            columns = row.find_all('td')
            koda = columns[0].get_text()
            datum = columns[1].get_text()
            stevilo = columns[2].get_text()
            data.append((koda, datum, stevilo))
    return data

def main():
    service = Service('/usr/local/bin/geckodriver')
    driver = webdriver.Firefox(service=service)
    wait = WebDriverWait(driver, 15)

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host='127.0.0.1',
        port=5432
    )
    cursor = conn.cursor()

    driver.get("https://atlas.ptice.si/atlas/index.php?r=user/login")
    driver.find_element(By.ID, "UserLogin_username").send_keys(ATLAS_USERNAME)
    driver.find_element(By.ID, "UserLogin_password").send_keys(ATLAS_PASSWORD)
    driver.find_element(By.ID, "UserLogin_rememberMe").click()
    driver.find_element(By.NAME, "yt0").click()
    time.sleep(3)

    base_dir = os.path.dirname(__file__)
    names_file = os.path.join(base_dir, "names.txt")
    tetrade_dir = os.path.join(base_dir, "tetrade")

    names = []
    with open(names_file, "r", encoding="utf-8") as file:
        for line in file:
            names.append(line.strip())

    driver.switch_to.new_window(WindowTypes.TAB)
    time.sleep(1)

    for name in names:
        table_name = name.replace(' ', '_').lower()
        create_species_table(cursor, name)
        tetrada_file = os.path.join(tetrade_dir, f"tetrade_{name}.txt")
        with open(tetrada_file, 'r') as file:
            for tetrada in file:
                tetrada = tetrada.strip()
                url2 = f"https://atlas.ptice.si/atlas/index.php?r=grafika/infowindow1&tetrada={tetrada}&vrsta_slo={name}&koda=>=-1%20&filter="
                driver.get(url2)
                try:
                    element = wait.until(EC.presence_of_element_located(
                        (By.XPATH, "/html/body/div/div/nav/ul/li[2]")
                    ))
                    element.click()
                except Exception as e:
                    print(f"Error finding the content element: {e}")
                    continue

                try:
                    podatki_m = wait.until(
                        EC.presence_of_element_located((By.ID, "content"))
                    ).text
                except Exception as e:
                    print(f"Error finding the content element: {e}")
                    continue

                data = parse(podatki_m)
                for row in data:
                    koda, datum, stevilo = row
                    insert_data_into_table(cursor, table_name, tetrada, koda, datum, stevilo)
                conn.commit()
        time.sleep(2)

    driver.quit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
