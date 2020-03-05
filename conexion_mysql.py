import mysql.connector

conn = mysql.connector.connect(
  host="localhost",
  database="api_zoho",
  user="api_zoho",
  passwd="api_zoho"
)
ejemplo_cursor= conn.cursor()
ejemplo_cursor.execute("show tables")


print (ejemplo_cursor.fetchall())

#print(conn)


