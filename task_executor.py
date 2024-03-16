from databricks import sql
import os

connection = sql.connect(server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                         http_path       = os.getenv("DATABRICKS_HTTP_PATH"),
                         access_token    = os.getenv("DATABRICKS_TOKEN"))

cursor = connection.cursor()

cursor.execute("SELECT * from range(10)")
print(cursor.fetchall())

cursor.close()
connection.close()
