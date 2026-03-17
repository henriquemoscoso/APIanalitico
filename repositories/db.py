# %%
import pyodbc 

# Servidor
server = "tcp:database.rmtecho.com.br,49775"
database = "Arthos.Events"
username = "sa"
password = "T8yP@2jK$4r"

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"   # útil em dev quando não há certificado confiável
)

def get_conn():
    # Aqui pode estourar erro de driver/credenciais/etc.
    try:
        conn = pyodbc.connect(conn_str)
        print("Conexão estabelecida.")
    except pyodbc.Error as e:
        # Re-raise com contexto mais claro (vai ser tratado no endpoint)
        raise RuntimeError("Falha ao conectar no SQL Server via ODBC.") from e
    else:
        return conn
# %%
get_conn()
# %%
