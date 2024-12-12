from cgi import print_exception

import pandas as pd
from db_connection import connect

# O CSV é um DataFrame de acordo com a biblioteca Pandas, logo seu uso mais comum é utilizando a váriavel 'df'
df = pd.read_csv('fake_csv.csv')

# Os dtypes do DataFrame do arquivo .CSV conterão algumas rows, então aqui eu devo iterar sobre elas
# Esse código me deu os nomes de cada coluna junto com seu tipo. (ex:
# Coluna: id_programa - Tipo: int64
# Coluna: curso - Tipo: object)

# for coluna in df.columns:
#     print(f"Coluna: {coluna} - Tipo: {df[coluna].dtype}")

# Essa função me retornou uma lista de tuplas onde cada tupla representa uma linha.
df_data = df.values.tolist()

def insert_data(data):
    conn = connect()
    cursor = conn.cursor()
    try:
        cursor.executemany(
            "INSERT INTO users (id, nome, cargo, departamento, salario, data_admissao, email, idade, cidade) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            data,
        )
        conn.commit()
        print(f"{cursor.rowcount} Dados inseridos com sucesso!")
    except Exception as e:
        if conn:
            conn.rollback()
        print(e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    insert_data(df_data)
