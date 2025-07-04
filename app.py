from fastapi import FastAPI
from typing import Optional
import sqlite3  
import pandas as pd


app = FastAPI(
    title="API",
    description="Questa API fornisce dati italiani sulla pesca",
    version="1.0.0"
)


def query_db(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect('pesca.db')  
    df = pd.read_sql_query(query, conn, params=params)  
    conn.close()
    return df


@app.get("/produttivita_nazionale")
def get_produttivita_totale_nazionale(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM produttivita_totale_nazionale"
    params = []  
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"  
        params.extend([da_anno, a_anno]) 
    df = query_db(query, tuple(params)) 
    return df.to_dict(orient='records')


@app.get("/produttivita_aree")
def get_produttivita_totale_aree(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM produttivita_totale_aree"
    params = []  
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"  
        params.extend([da_anno, a_anno]) 
    df = query_db(query, tuple(params)) 
    return df.to_dict(orient='records')


@app.get("/valore_aggiunto_aree")
def get_valore_aggiunto_aree(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM valore_aggiunto_aree"
    params = []  
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"  
        params.extend([da_anno, a_anno]) 
    df = query_db(query, tuple(params)) 
    return df.to_dict(orient='records')


@app.get("/occupazione_nazionale")
def get_occupazione_nazionale(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM occupazione_percentuale_nazionale"
    params = []  
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"  
        params.extend([da_anno, a_anno]) 
    df = query_db(query, tuple(params)) 
    return df.to_dict(orient='records')


@app.get("/occupazione_aree")
def get_occupazione_aree(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM occupazione_percentuale_aree"
    params = []  
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"  
        params.extend([da_anno, a_anno]) 
    df = query_db(query, tuple(params)) 
    return df.to_dict(orient='records')