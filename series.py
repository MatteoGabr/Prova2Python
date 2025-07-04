import sqlite3 
import pandas as pd 

def query_db(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect('pesca.db')  
    df = pd.read_sql_query(query, conn, params=params)  
    conn.close() 
    return df


conn = sqlite3.connect('pesca.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS produttivita_totale_nazionale (
    Anno INTEGER PRIMARY KEY,
    Produttivita_Totale FLOAT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS produttivita_totale_aree (
    Anno INTEGER PRIMARY KEY,
    Produttivita_Totale FLOAT
    Area VARCHAR(50)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS valore_aggiunto_aree (
    Anno INTEGER PRIMARY KEY,
    Valore_Aggiunto FLOAT
    Area VARCHAR(50)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS occupazione_percentuale_nazionale (
    Anno INTEGER PRIMARY KEY,
    Occupazione_Totale FLOAT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS occupazione_percentuale_aree (
    Anno INTEGER PRIMARY KEY,
    Occupazione_Totale FLOAT
)
''')

query_join = '''
SELECT p.Anno, p.Produttivita, r.Area_Geografica
FROM produttivita_settore_pesca p
JOIN regioni r ON p.Regione_id = r.id
'''

query_join_valore_aggiunto = '''
SELECT p.Anno, p.Percentuale, r.Area_Geografica
FROM importanza_settore_pesca p
JOIN regioni r ON p.Regione_id = r.id
'''

query_join_occupazione = '''
SELECT p.Anno, p.Percentuale, r.Area_Geografica
FROM occupazione_settore_pesca p
JOIN regioni r ON p.Regione_id = r.id
'''

df_produttivita_joined = query_db(query_join)
df_importanza_joined = query_db(query_join_valore_aggiunto)
df_occupazione_joined = query_db(query_join_occupazione)


produttivita_totale_nazionale = df_produttivita_joined.groupby(['Anno'])['Produttivita'].sum().round(2).reset_index()
produttivita_totale_aree = df_produttivita_joined.groupby(['Anno', 'Area_Geografica'])['Produttivita'].sum().round(2).reset_index()

importanza_valore_aggiunto_aree = df_importanza_joined.groupby(['Anno', 'Area_Geografica'])['Percentuale'].mean().round(2).reset_index()

occupazione_totale_nazionale = df_occupazione_joined.groupby(['Anno'])['Percentuale'].mean().round(2).reset_index()
occupazione_totale_aree = df_occupazione_joined.groupby(['Anno', 'Area_Geografica'])['Percentuale'].mean().round(2).reset_index()


produttivita_totale_nazionale.to_sql(
    'produttivita_totale_nazionale',
    conn,
    if_exists='replace',
    index=False
)

produttivita_totale_aree.to_sql(
    'produttivita_totale_aree',
    conn,
    if_exists='replace',
    index=False
)

importanza_valore_aggiunto_aree.to_sql(
    'valore_aggiunto_aree',
    conn,
    if_exists='replace',
    index=False
)

occupazione_totale_nazionale.to_sql(
    'occupazione_percentuale_nazionale',
    conn,
    if_exists='replace',
    index=False
)

occupazione_totale_aree.to_sql(
    'occupazione_percentuale_aree',
    conn,
    if_exists='replace',
    index=False
)

conn.close()