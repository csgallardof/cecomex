#!/usr/bin/env python
# coding: utf-8

# In[91]:


import pyodbc
import pandas as pd
import numpy as np
import datetime
from datetime import datetime
from datetime import timedelta
conn = pyodbc.connect('DSN=MBAV17')
curs = conn.cursor()


# In[92]:
from zcrmsdk import ZCRMRecord ,ZCRMRestClient, ZCRMModule, ZohoOAuth




    

def leerTablas(tabla,campo,where,group, order):
    texto='''SELECT {blancoCampo}
FROM {blancoTabla}
{blancoWhere}
{blancoGroup}
{blancOrder}
'''.format(blancoTabla=tabla, blancoCampo=campo,blancoWhere=where, blancoGroup=group, blancOrder=order)
    test= curs.execute(texto).fetchall()
    return(test)


# In[93]:


campos=['CODIGO_CLIENTE','IDENTIFICACION_FISCAL']
nombreTabla='CLNT_Ficha_Principal'
whereStatement="WHERE (`EMPRESA`='CECO2') "
groupStatement=""
orderStatement=""

tablaPrin = pd.DataFrame()
for c in campos:
    tablaPrin[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaPrin[c])):
        tablaPrin[c][i]=tablaPrin[c][i][0]
    print(c, end=' | ')
print('\nCargados '+str(len(tablaPrin['CODIGO_CLIENTE']))+' clientes')


# REMOVER CLIENTES QUE NO TIENEN FACTURAS POSTERIOR AL '2016-01-01'

# In[94]:


campos=['CODIGO_CLIENTE','MAX(FECHA_FACTURA)']
nombreTabla='CLNT_Factura_Principal'
whereStatement="WHERE EMPRESA='CECO2' AND ANULADA=False" #AND FECHA_FACTURA>'2016-01-01'"
groupStatement="GROUP BY CODIGO_CLIENTE"
orderStatement=""

print('Removiendo clientes no activos:')
tablaSec = pd.DataFrame()
listaActivos=[]
for c in campos:
    tablaSec[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaSec[c])):
        tablaSec[c][i]=tablaSec[c][i][0]
        listaActivos.append(tablaSec[c][i])
    print(c, end=' | ')

for i in range(len(tablaPrin)):
    if tablaPrin['CODIGO_CLIENTE'][i] in listaActivos:
        pass
    else:
        tablaPrin = tablaPrin.drop([i], axis=0)

tablaPrin.reset_index(inplace = True, drop=True) 
tablaPrin=pd.merge(tablaPrin, tablaSec, how='left', left_on='CODIGO_CLIENTE', right_on='CODIGO_CLIENTE')
print('\nQuedan '+ str(len(tablaPrin))+' clientes restantes')


# PROMEDIO VENTAS MENSUAL ÚLTIMOS 6 MESES

# In[95]:


#meses para el cálculo:
meses=6
def monthdelta(date, delta):
    m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
    if not m: m = 12
    d = min(date.day, [31,
        29 if y%4==0 and not y%400==0 else 28,31,30,31,30,31,31,30,31,30,31][m-1])
    return date.replace(day=d,month=m, year=y)

pasado=monthdelta(datetime.now(), -meses)
pasado=pasado.strftime("%d/%m/%Y")

campos=['CODIGO_CLIENTE', 'SUM(VALOR_FACTURA)/{blanco}'.format(blanco=meses)]
nombreTabla='CLNT_Factura_Principal'
whereStatement="WHERE EMPRESA='CECO2' AND ANULADA=False AND FECHA_FACTURA>'{blanco}'".format(blanco=pasado)
groupStatement="GROUP BY CODIGO_CLIENTE"
orderStatement=""

tablaSec = pd.DataFrame()
for c in campos:
    tablaSec[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaSec[c])):
        tablaSec[c][i]=tablaSec[c][i][0]
    print(c, end=' | ')

tablaPrin=pd.merge(tablaPrin, tablaSec, how='left', left_on='CODIGO_CLIENTE', right_on='CODIGO_CLIENTE')
print('\nCalculado Promedio de Ventas Mensual')


# DÍAS FACTURA VENCIDA

# In[96]:


hoy=datetime.today().date()

campos=['CODIGO_CLIENTE', 'min(FECHA_VENCIMIENTO)']
nombreTabla='CLNT_Factura_Principal'
#whereStatement="WHERE EMPRESA= 'CECO2' and ANULADA=False and VALOR_TOTAL_SALDO_A_COBRAR>0 and FECHA_VENCIMIENTO<='{blanco}'".format(blanco=hoy)
whereStatement="WHERE EMPRESA= 'CECO2' and ANULADA=False and VALOR_TOTAL_SALDO_A_COBRAR>0 "
groupStatement="GROUP BY CODIGO_CLIENTE"
orderStatement=""

tablaSec = pd.DataFrame()
for c in campos:
    tablaSec[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaSec[c])):
        tablaSec[c][i]=tablaSec[c][i][0]
    print(c, end=' | ')

    
ddif=[]
for i in range(len(tablaSec)):
    ddif.append((hoy-tablaSec['min(FECHA_VENCIMIENTO)'][i]).days)
tablaSec['Días factura vencida']=ddif
    

"""dias=[]
for i in range(len(tablaPrin)):
    try:
        dias.append(tablaSec['Días factura vencida'][tablaSec.index[tablaSec['CODIGO_CLIENTE']==tablaPrin['CODIGO_CLIENTE'][i]][0]])
    except:
        dias.append(0)
tablaPrin['Días factura vencida']=dias """

tablaPrin=pd.merge(tablaPrin, tablaSec, how='left', left_on='CODIGO_CLIENTE', right_on='CODIGO_CLIENTE')
tablaPrin=tablaPrin.drop('min(FECHA_VENCIMIENTO)', axis=1)
print('\nCalculados los días de factura vencida')


# SALDO PENDIENTE

# In[97]:


campos=['CODIGO_CLIENTE','SUM(VALOR_TOTAL_SALDO_A_COBRAR)']
nombreTabla='CLNT_Factura_Principal'
whereStatement="WHERE EMPRESA= 'CECO2'  and ANULADA=False"
groupStatement="GROUP BY CODIGO_CLIENTE"
orderStatement=""

tablaSec = pd.DataFrame()
for c in campos:
    tablaSec[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaSec[c])):
        tablaSec[c][i]=tablaSec[c][i][0]
    print(c, end=' | ')

"""saldo=[]
for i in range(len(tablaPrin)):
    try:
        saldo.append(tablaSec['SUM(VALOR_TOTAL_SALDO_A_COBRAR)'][tablaSec.index[tablaSec['CODIGO_CLIENTE']==tablaPrin['CODIGO_CLIENTE'][i]][0]])
    except:
        saldo.append(0)

tablaPrin['Saldo Pendiente']=saldo"""
tablaPrin=pd.merge(tablaPrin, tablaSec, how='left', left_on='CODIGO_CLIENTE', right_on='CODIGO_CLIENTE')
print('\nCalculado el Saldo Pendiente')


# PROMEDIO DIAS VENCIDOS

# In[98]:


añoAtras=monthdelta(hoy,-12)

campos=['CODIGO_CLIENTE', 'MAX(FECHA_COBRO)', 'MAX(FECHA_VENCIMIENTO_FACTURA)']
nombreTabla='CLNT_Cobro_Detalle'
whereStatement="WHERE EMPRESA= 'CECO2'  and FECHA_VENCIMIENTO_FACTURA>'{blanco}' and VALOR_SALDO=0".format(blanco=añoAtras)
groupStatement="GROUP BY CODIGO_FACTURA, CODIGO_CLIENTE"
orderStatement=""

tablaSec = pd.DataFrame()
for c in campos:
    tablaSec[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaSec[c])):
        tablaSec[c][i]=tablaSec[c][i][0]
    print(c, end=' | ')


retraso=[]
clientes=[]
for i in range(len(tablaSec)):
    tablaSec['CODIGO_CLIENTE'][i]=tablaSec['CODIGO_CLIENTE'][i][:-6]
    clientes.append(tablaSec['CODIGO_CLIENTE'][i])
    try:
        retraso.append((tablaSec['MAX(FECHA_COBRO)'][i]-tablaSec['MAX(FECHA_VENCIMIENTO_FACTURA)'][i]).days)
    except:
        retraso.append(0)

tablaSec['Retraso']=retraso

clientes=list(dict.fromkeys(clientes))

promedio=[]
for cliente in clientes:
    seleccionado=tablaSec['CODIGO_CLIENTE']==cliente
    promedio.append(round(sum(tablaSec[seleccionado]['Retraso'])/len(tablaSec[seleccionado])))


promVen=[]
for i in tablaPrin['CODIGO_CLIENTE']:
    try:
        promVen.append(promedio[clientes.index(i)])
    except:
        promVen.append(0)

tablaPrin['Promedio Días Vencidos']=promVen
print('\nCalculado Promedio Días Vencidos ') 


# FRECUENCIA DE COMPRA

# In[99]:


campos=['CODIGO_CLIENTE','FECHA_FACTURA']
nombreTabla='CLNT_Factura_Principal'
whereStatement="WHERE EMPRESA='CECO2' AND ANULADA=False AND FECHA_FACTURA>'{blanco}'".format(blanco=añoAtras)
groupStatement=""
orderStatement=""

tablaSec = pd.DataFrame()
for c in campos:
    tablaSec[c]=leerTablas(nombreTabla, c,whereStatement,groupStatement,orderStatement)
    for i in range(len(tablaSec[c])):
        tablaSec[c][i]=tablaSec[c][i][0]
    print(c, end=' | ')
    
def average(lst): 
    return sum(lst) / len(lst) 

frecuencia=[]
for cliente in clientes:
    try:
        seleccionado=tablaSec['CODIGO_CLIENTE']==cliente
        primero=1
        regDif=[]
        for i in tablaSec[seleccionado]['FECHA_FACTURA']:
            if primero==1:
                anterior=i
                primero =0
            else:
                diferencia=(i-anterior).days
                regDif.append(diferencia)
                anterior=i
        frecuencia.append(round(average(regDif)))
    except:
        frecuencia.append(0)

freq=[]
for i in tablaPrin['CODIGO_CLIENTE']:
    try:
        freq.append(frecuencia[clientes.index(i)])
    except:
        freq.append(0)
tablaPrin['Frecuencia de compra']=freq
print('\nCalculada Frecuencia de Compra')


# CAMBIAR NOMBRES DE COLUMNAS

# In[100]:


tablaPrin.columns=['ID de Cliente','RUC/CI','Fecha última compra',
                   'Promedio de Ventas Mensual','Días factura vencida (mora)',
                   'Saldo Pendiente','Promedio de Días de vencimiento','Promedio días entre compras']
tablaPrin=tablaPrin.fillna(0)


# CREAR TABLA DEFINITIVA

# In[101]:


#tablaPrin.to_csv(r'C:\Users\jefe.proyectos\Documents\Tablas MBA\Para sincronizar\estadoClientes.csv', index = False)
#tablaPrin.to_excel(r'C:\Users\jefe.proyectos\Documents\Tablas MBA\Para sincronizar\estadoClientes.xlsx')
import mysql.connector
conn = mysql.connector.connect(host="localhost",database="cecomex_final",user="root",passwd="")
c= conn.cursor()
data=tablaPrin[['RUC/CI','Fecha última compra','Días factura vencida (mora)','Saldo Pendiente','Promedio de Días de vencimiento','Promedio días entre compras','Promedio de Ventas Mensual']].to_numpy()
print('mysql')

configuration_dictionary = { 
    "sandbox":"false",
    "applicationLogFilePath":"./logins",
    "currentUserEmail": "jefe.proyectos@cecomex.com.ec",
    "client_id": "1000.YVMNLP5AO3Q8TK6V0KI0H62P5X22QH",
    "client_secret": "b3fc31536fb55e41140e7ddf1b38eecf8172fa611b",
    "redirect_uri": "http://www.zoho.com",
    "token_persistence_path": "./TokenPersistence"
    }

    
ZCRMRestClient.initialize(configuration_dictionary)

selectsql="select RUC,entity_id from cliente"        
c.execute(selectsql)
select=c.fetchall()

codigo={}
for i in select:
    codigo[i[0]]=i[1]

listruc=[]
for ruc,f,df,sp,dv, dc,vm in data:
    if ruc in codigo.keys():
        sql="update cliente set Fecha_Ult_Comp=%s,Dias_factura_vencida_mora=%s,Saldo_Pendiente=%s,Promedio_dias_de_vencimiento=%s, Promedias_entre_compras=%s,Promedio_de_Ventas_Mensual=%s where RUC='%s'"%(f,df,sp,dv,dc,vm,ruc)
        
        #c.execute(sql)

        codex=codigo[str(ruc)]
        record=ZCRMRecord.get_instance('Accounts',codex)
        record.set_field_value('Fecha_ltima_compra1', str(f))
        record.set_field_value('D_as_factura_vencida_mora', int(df))
        record.set_field_value('Saldo_Pendiente', round(sp,2))
        record.set_field_value('Promedio_de_D_as_de_vencimiento',int(dv))
        record.set_field_value('Promedio_d_as_entre_compras', int(dc))
        record.set_field_value('Promedio_de_Ventas_Mensual', round(vm,2))
        record.set_field_value('RUC_CI',ruc)
        resp=record.update()
    else:
        listruc.append(ruc)
        print(ruc)
            #print(ruc)
        
        #print('ruc %s'%ruc)
    #    print('error '+sql) 
     #print(ruc,dv,dc,vm)
conn.commit()
print('Listo!')


# In[ ]

