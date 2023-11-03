import csv
import sqlite3

with open('producao_alimentos.csv', 'r', encoding='utf-8') as file:
    
    reader = csv.reader(file)
    next(reader)
    
    con = sqlite3.connect('dsadb.db')
    
    con.execute('DROP TABLE IF EXISTS producao')
    con.execute('''
                CREATE TABLE producao (
                produto TEXT,
                quantidade INTEGER,
                preco_medio REAL,
                receita_total REAL
                )''')
    
    for row in reader:
        if int(row[1]) > 10:
            con.execute('INSERT INTO producao (produto, quantidade, preco_medio, receita_total) VALUES (?, ?, ?, ?)', row)
        
    con.commit()
    con.close()

print('Job concluido com sucesso')