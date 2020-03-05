import pyodbc
conn = pyodbc.connect('DSN=MBA')
#conn = pyodbc.connect('Driver={4D v17 ODBC Driver 64-bit} ;Server=192.168.254.13 ;UID=API ;PWD=API')
#conn = pyodbc.connect('Driver={4D v17 ODBC Driver 64-bit} ;Server=w2k12srvcecomex; DATABASE=MBA3  v17 4 e44 Preview ;UID=API ;PWD=API')

curs = conn.cursor()


curs.execute('select (CORP),(PRODUCT_ID),(PRODUCT_NAME), (PRODUCT_ID_CORP), (UM), (AVAILABLE) from INVT_FICHA_PRINCIPAL')
test=curs.fetchall()
test.sort()
print(test)