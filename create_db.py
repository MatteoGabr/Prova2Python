import sqlite3 

conn = sqlite3.connect('pesca.db')

conn.execute('''
CREATE TABLE IF NOT EXISTS occupazione_settore_pesca (
    ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,  
    Anno INT NOT NULL,
    Regione_id INT NOT NULL,                  
    Percentuale DOUBLE NOT NULL         
)
''')

conn.execute('''
CREATE TABLE IF NOT EXISTS importanza_settore_pesca (
    ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,  
    Anno INT NOT NULL,
    Regione_id INT NOT NULL,                  
    Percentuale DOUBLE NOT NULL         
)
''')

conn.execute('''
CREATE TABLE IF NOT EXISTS produttivita_settore_pesca (
    ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,  
    Anno INT NOT NULL,
    Regione_id INT NOT NULL,                  
    Produttivita DOUBLE NOT NULL         
)
''')

conn.execute('''
CREATE TABLE IF NOT EXISTS regioni (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,   
    Nome VARCHAR(50) NOT NULL,                   
    Area_Geografica TEXT NOT NULL    
)
''')

regioni = [
    ('Valle d\'Aosta', 'Nord-ovest'),
    ('Piemonte', 'Nord-ovest'),
    ('Liguria', 'Nord-ovest'),
    ('Lombardia', 'Nord-ovest'),
    ('Trentino-Alto Adige', 'Nord-est'),
    ('Veneto', 'Nord-est'),
    ('Friuli-Venezia Giulia', 'Nord-est'),
    ('Emilia-Romagna', 'Nord-est'),
    ('Toscana', 'Centro'),
    ('Umbria', 'Centro'),
    ('Marche', 'Centro'),
    ('Lazio', 'Centro'),
    ('Abruzzo', 'Centro'),
    ('Molise', 'Sud'),
    ('Campania', 'Sud'),
    ('Puglia', 'Sud'),
    ('Basilicata', 'Sud'),
    ('Calabria', 'Sud'),
    ('Sicilia', 'Isole'),
    ('Sardegna', 'Isole')
]

conn.executemany('INSERT INTO regioni (Nome, Area_Geografica) VALUES (?, ?)', regioni)

print("Tabelle create e popolamento regioni completato con successo.")

conn.commit()

conn.close()