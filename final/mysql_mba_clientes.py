import mysql.connector
import pyodbc

conn = mysql.connector.connect(host="localhost",database="cecomex_final",user="root",passwd="#C3c0meX")
    
c= conn.cursor()
"""
0.Account_Name
1.RUC
2.Industry -> CODIGO_GIRO
3.owner_id -> SALESMAN
4.Phone
5.Shipping_State
6.Shipping_Street ->DIRECCION_PRINCIPAL_1
7.Transporte_preferido -> CODIGO_TRANSPORTE_DESP
8.Rating 
9.Limite_de_Credito 
10.Dias_de_Pago_2
11.Correo_Electronico_Fact -> Fiscal
12.Correo_electronico -> EMAIL
13.Shipping_City ->CIUDAD_DESPACHO
14.mensaje general  -> alert
15.Zona
16.Regimen_Fiscal
17.Referencia
"""
c.execute("select Account_Name,RUC,Industry,owner_id,Phone,Shipping_State,\
	Shipping_Street,Transporte_preferido,Rating, 	Limite_de_Credito , \
	Dias_de_Pago, Correo_Electronico_Fact,Correo_electronico,  \
	Shipping_City,alert,ZONA,Regimen_Fiscal,Referencia from cliente")
mydb=c.fetchall()

conmba = pyodbc.connect('DSN=MBAV17')
curs = conmba.cursor()
curs.execute("select (IDENTIFICACION_FISCAL),(CODIGO_CLIENTE),(NOMBRE_CLIENTE),(IDENTIFICACION_FISCAL) from CLNT_FICHA_PRINCIPAL WHERE EMPRESA='CECO2'")

#SIS_Lista_6  
#SIST_Lista_1.DESCRIPTION_ENG SALESMAN
#SIST_Lista_3.CODIGO_TRANSPORTE_DESP
exists={}
code=[]
select=curs.fetchall()
#blacklist={}

for i in select:
	exists[i[0]]=i[1]
	code.append(i[1])
	#if i[1][0:2]!='C0': 
		#blacklist[i[0]]=i[1]
code=list(filter(lambda i:'C0' ==i[0:2],code)) 
code.sort()
code.reverse()
id=int(code[0][1:])+1
print(id)
curs.execute("select (CODE),(DESCRIPTION_ENG) from SIST_Lista_6 WHERE CORP='CECO2' AND GROUP_CATEGORY='GIRNE'")
sis6=curs.fetchall()
industry={}
for i in sis6:
	industry[i[1]]=i[0]

curs.execute("select (CODE),(TEXTO1_20) from SIST_Lista_3 WHERE CORP='CECO2' AND `GROUP CATEGORY`='TRANS'")
sis3=curs.fetchall()
transport={}
for i in sis3:
	transport[i[1]]=i[0]
indus=None
trans=None

count=0
print("Start wrting MBA")
for i in mydb:
	try:
		indus=industry[i[2]]
	except:
		indus=i[2]
	try:
		trans=transport[i[7]]
	except:
		trans=i[7]

	if  i[1] in exists.keys():
		query="UPDATE CLNT_FICHA_PRINCIPAL SET\
		SUCURSAL='PRI',Codigo_Cuenta_Contable_Cliente='10100200001',\
		CODIGO_TRANSPORTE_DESP='%s',CODIGO_GIRO='%s',\
		TELEFONO='%s',ESTADO='%s',\
		DIRECCION_DESPACHO_1='%s',DIRECCION_PRINCIPAL_1='%s',CLIENT_TYPE='%s', LIMITE_CREDITO=%s,\
		TERMINOS_DE_PAGO_DIAS=%s,TERMINOS_DE_PAGO_ALFA_NUM='%s' , EMPRESA='CECO2',E_MAIL='%s',\
		CIUDAD_DESPACHO='%s',CIUDAD_PRINCIPAL='%s',NOMBRE_CLIENTE='%s',ZONA='%s', Codigo_RegimenFiscal='%s', Referencia_s='%s'\
		WHERE CODIGO_CLIENTE='%s'"\
		%(trans.upper(),indus.upper(),i[4].upper(),i[5].upper(),\
			i[6].upper(),i[6].upper(),i[8].upper(),i[9].upper(),\
			i[10].upper(),str(i[10])+' DIAS',i[12],i[13].upper(),i[13].upper(),i[0].upper(),i[15],i[16],i[17],exists[i[1]])
		#print(query)
		try:
			curs.execute(query)
		except:
			print(query)
		"""
		try: 
			codex='C'+str(id+10000000)[1:]
			query="UPDATE CLNT_FICHA_PRINCIPAL SET SUCURSAL='Principal - Matriz',Codigo_Cuenta_Contable_Cliente='10100200001',CODIGO_CLIENTE='%s',CODIGO_TRANSPORTE_DESP='%s',CODIGO_GIRO='%s',TELEFONO='%s',ESTADO='%s',DIRECCION_DESPACHO_1='%s',CLIENT_TYPE='%s', LIMITE_CREDITO=%s, TERMINOS_DE_PAGO_DIAS=%s , EMPRESA='CECO2',Email_Fiscal='%s' WHERE IDENTIFICACION_FISCAL='%s'"%(codex,trans,indus,i[4],i[5],i[6],i[7],i[9],i[10],i[11],i[1])
			id+=1	
		except:
			False
		"""
		
	else:	
		codex='C'+str(id+10000000)[1:]
		exists[i[1]]=codex
		codigo=codex+'-CECO2'
		query="INSERT INTO CLNT_FICHA_PRINCIPAL (SUCURSAL,Codigo_Cuenta_Contable_Cliente,\
		CODIGO_CLIENTE,NOMBRE_CLIENTE,IDENTIFICACION_FISCAL,\
		TELEFONO,ESTADO,DIRECCION_DESPACHO_1,DIRECCION_PRINCIPAL_1,\
		CLIENT_TYPE,EMPRESA,CODIGO_TRANSPORTE_DESP,CODIGO_GIRO,LIMITE_CREDITO,\
		TERMINOS_DE_PAGO_DIAS,TERMINOS_DE_PAGO_ALFA_NUM,Email_Fiscal,E_MAIL,\
		CIUDAD_DESPACHO,CIUDAD_PRINCIPAL,CODIGO_CLIENTE_EMPRESA,TIPO_MONEDA, \
		ZONA,Codigo_RegimenFiscal,Referencia_s)\
		VALUES ('PRI','10100200001','%s','%s','%s',\
		'%s','%s','%s','%s',\
		'%s','CECO2','%s','%s',%s,\
		%s,'%s','%s','%s','%s','%s','%s','US',\
		'%s','%s','%s')"\
		%(codex,i[0].upper(),i[1].upper(),i[4].upper(),i[5].upper(),i[6].upper(),i[6].upper(),i[8].upper(),trans.upper(),indus.upper(),i[9].upper(),i[10],str(i[10])+' DIAS',i[11],i[12],i[13].upper(),i[13].upper(),codigo,i[15],i[16],i[17])
		id=id+1
		curs.execute(query)
		print(query)

	count+=1
	if count%200==0:
		print(query,' numero ',count)


	if i[1]=="1308941416001" or i[1]=="1708514847001" or  i[1]=="1333333330001":
		print(i[1],query)
		#Codigo_Cuenta_Contable_Cliente: '10100200001'
		#SUCURSAL: 'Principal - Matriz'
		#EMPRESA: 'CECO2'
		#TIPO_MONEDA: 'US'
		
		"""
		duplicados
		curs.execute("SELECT  CODIGO_CLIENTE,COUNT(CODIGO_CLIENTE) FROM CLNT_FICHA_PRINCIPAL GROUP BY CODIGO_CLIENTE HAVING COUNT(CODIGO_CLIENTE)>1") 
		d=curs.fetchall()
		for i in d:
			curs.execute("DELETE FROM CLNT_FICHA_PRINCIPAL WHERE CODIGO_CLIENTE='%s'%i[0]")
		"""
print("End writing MBA")
