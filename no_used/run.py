from lib.connection import get_connection

connection = get_connection()
cursor = connection.cursor()

cursor.execute("SELECT * from range(11)")
print(cursor.fetchall())

cursor.close()
connection.close()
