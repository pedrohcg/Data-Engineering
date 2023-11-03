import csv
import sqlite3

def remove_ponto(valor):
    return int(valor.replace('.', ''))

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
                receita_total INTEGER,
                margem_lucro REAL
                )''')
    
    for row in reader:
        if int(row[1]) > 10:
            row[-1] = remove_ponto(row[-1])
            margem_lucro = round((row[-1]/ float(row[1])) - float(row[2]), 2)
            con.execute('INSERT INTO producao (produto, quantidade, preco_medio, receita_total, margem_lucro) VALUES (?, ?, ?, ?, ?)', (row[0], row[1], row[2], row[3], margem_lucro))
        
    con.commit()
    con.close()

print('Job concluido com sucesso')