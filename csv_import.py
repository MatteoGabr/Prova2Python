import os
import requests
from io import StringIO
import pandas as pd
import sqlite3

url_occcupazione = 'https://raw.githubusercontent.com/MatteoGabr/progetto1_gabrielli/main/Andamento-occupazione-del-settore-della-pesca-per-regione.csv'
url_importanza = 'https://raw.githubusercontent.com/MatteoGabr/progetto1_gabrielli/main/Importanza-economica-del-settore-della-pesca-per-regione.csv'
url_produttivita = 'https://raw.githubusercontent.com/MatteoGabr/progetto1_gabrielli/main/Produttivita-del-settore-della-pesca-per-regione.csv'

curr_dir = os.getcwd()

csv_dir = os.path.join(curr_dir, 'csv')

def import_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        csv_content = StringIO(response.text)
        df = pd.read_csv(csv_content, sep=';', na_values=['', 'Null'])
        # Corregge caratteri speciali nei nomi delle colonne
        df.columns = [col.replace('�', 'à') for col in df.columns]
        df.columns = [col.replace('ŕ', 'à') for col in df.columns]
        return df
    else:
        print(f"Errore nell'importazione dei dati da {url}")
        return None
    

def save_df_local(df, filename):
    os.makedirs(csv_dir, exist_ok=True)  # Crea la cartella se non esiste
    path = os.path.join(csv_dir, filename)
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"DataFrame salvato in {path}")


def interpolate_missing_data(df, columns):
    for col in columns:
        df[col] = df[col].interpolate(method='linear')
    return df


df_occupazione = import_data(url_occcupazione)
df_importanza = import_data(url_importanza)
df_produttivita = import_data(url_produttivita)

interpolate_missing_data(df_occupazione, ['Variazione percentuale unità di lavoro della pesca'])
interpolate_missing_data(df_importanza, ['Percentuale valore aggiunto pesca-piscicoltura-servizi'])
interpolate_missing_data(df_produttivita, ['Produttività in migliaia di euro'])

save_df_local(df_occupazione, "occupazione_settore_pesca.csv")
save_df_local(df_importanza, "importanza_settore_pesca.csv")
save_df_local(df_produttivita, "produttivita_settore_pesca.csv")


conn = sqlite3.connect('pesca.db')
cursor = conn.cursor()

for _, row in df_occupazione.iterrows():
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    cursor.execute(
        '''
        INSERT INTO occupazione_settore_pesca (Anno, Regione_id, Percentuale)
        VALUES (?, ?, ?)
        ''',
        (row['Anno'], regione_id, row['Variazione percentuale unità di lavoro della pesca'])
    )

for _, row in df_importanza.iterrows():
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    cursor.execute(
        '''
        INSERT INTO importanza_settore_pesca (Anno, Regione_id, Percentuale)
        VALUES (?, ?, ?)
        ''',
        (row['Anno'], regione_id, row['Percentuale valore aggiunto pesca-piscicoltura-servizi'])
    )

for _, row in df_produttivita.iterrows():
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    cursor.execute(
        '''
        INSERT INTO produttivita_settore_pesca (Anno, Regione_id, Produttivita)
        VALUES (?, ?, ?)
        ''',
        (row['Anno'], regione_id, row['Produttività in migliaia di euro'])
    )


conn.commit()

conn.close()