from databricks import sql
import os

def get_connection():
    connection = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                            http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                            access_token    = os.getenv("DATABRICKS_TOKEN"))
    
    return connection