import sys
from colorama import init, Fore
import pandas as pd
from db_connection import connect

# O CSV é um DataFrame de acordo com a biblioteca Pandas,
# logo seu uso mais comum é utilizando a váriavel 'df'
# df = pd.read_csv('fake_csv.csv')

# Os dtypes do DataFrame do arquivo .CSV conterão algumas rows,
# então aqui eu devo iterar sobre elas
# Esse código me deu os nomes de cada coluna junto com seu tipo. (ex:
# Coluna: id_programa - Tipo: int64
# Coluna: curso - Tipo: object)

# for coluna in df.columns:
#     print(f"Coluna: {coluna} - Tipo: {df[coluna].dtype}")

# Essa função me retornou uma lista com dicionários
# onde cada dicionário(objeto) representa uma linha.
# df_data = df.values.tolist()


def validate_csv(df):
    try:
        # TODO Adequar qual colunas são necessárias para executar as queries.
        required_columns = ['cpf', 'id_curso_turno', 'id_programa_unidade']

        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Coluna '{column}' não encontrada no arquivo CSV")

        if df.empty:
            raise ValueError("O arquivo CSV está vazio")

        return True
    except Exception as e:
        print(Fore.RED + f'Erro na validação do CSV: {str(e)}\n' + Fore.RESET)
        return False


def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)

        if not validate_csv(df):
            sys.exit(1)

        return df
    except FileNotFoundError:
        print(Fore.RED + f'Arquivo não encontrado: {file_path}\n' + Fore.RESET)
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(Fore.RED + 'O arquivo CSV está vazio.\n' + Fore.RESET)
        sys.exit(1)
    except pd.errors.ParserError:
        print(Fore.RED + 'Erro ao ler o arquivo CSV. Verifique o formato.\n' + Fore.RESET)
        sys.exit(1)
    except Exception as e:
        print(Fore.RED + f'Erro inesperado ao ler CSV: {str(e)}\n' + Fore.RESET)
        sys.exit(1)


def connect_db():
    try:
        conn = connect()
        return conn
    except Exception as e:
        print(Fore.RED + f'Não foi possível se conectar ao Banco de Dados: {str(e)}\n' + Fore.RESET)
        sys.exit(1)


def insert_data(data):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        if not data:
            print(Fore.RED + 'Nenhum dado para inserir.\n' + Fore.RESET)
        else:
            print(Fore.BLUE + cursor.rowcount + 'Dados estão na tabela' + Fore.RESET)
            cursor.executemany('INSERT INTO sis.setrab.tabela_temporaria (cpf, id_curso_turno, id_programa_unidade) VALUES (%s, %s, %s)', data)
            conn.commit()
            print(Fore.GREEN + f"{cursor.rowcount} Dados inseridos com sucesso!\n" + Fore.RESET)
    except Exception as e:
        if conn:
            conn.rollback()
        print(Fore.RED + f'Erro ao inserir dados: {str(e)}\n' + Fore.RESET)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def show_invalid_cpf(data):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        db_data = cursor.execute('SELECT tt.cpf, "CPF inválido" as situacao, p.nome as programa '
                                 'FROM sis_setrab.tabela_temporaria tt '
                                 'JOIN sis_setrab.programa p '
                                 'ON p.id = tt.id_programa where formatcpfcnpj(cpf) is null')
        db_data.to_csv('invalid_cpf.csv', index=False)
    except Exception as e:
        if conn:
            conn.rollback()
        print(Fore.RED + f'Erro ao selecionar dados: {str(e)}\n' + Fore.RESET)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    init()
    confirmacao = input(
        Fore.BLUE + 'O formato do arquivo está de acordo com'
                    ' as especificações necessárias (s/N):\n' + Fore.RESET)

    if confirmacao.lower() not in ['s', 'y', 'sim']:
        print(Fore.RED + 'O Script foi abortado com sucesso.' + Fore.RESET)
        sys.exit(0)

    try:
        # TODO Arrumar arquivo para execução do script
        df = read_csv('fake_csv.csv')
        df_data = df.values.tolist()

        print(Fore.GREEN + 'Executando Script...' + Fore.RESET)
        insert_data(df_data)
    except Exception as e:
        print(Fore.RED + f'Erro durante a execução do script: {str(e)}\n' + Fore.RESET)
        sys.exit(1)
