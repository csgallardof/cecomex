import mysql.connector
import itertools

from zcrmsdk import ZCRMRecord ,ZCRMRestClient, ZCRMModule, ZohoOAuth

# ETL ZohoCRM - Python - MySQL

if __name__ == "__main__":
    # Conexion BD (MySQL)
    db_name = 'cecomex_final'
    host = "localhost"
    user = "root"
    password = "#C3c0meX"
    conn = mysql.connector.connect(host="localhost",database="cecomex_final",user="root",passwd=password)    
    c= conn.cursor()
    
    c.execute("select RUC from cliente")
    mydb=c.fetchall()
    ruc=list(itertools.chain(*mydb))

    # Insertar registros dentro de la base de datos 
    insertFormula = "INSERT INTO cliente (entity_id, owner_id, \
                Fecha_Ult_Comp, Limite_de_Credito, Industry, Estado_del_Cliente, \
                Promedio_de_Ventas_Mensual, Dias_Inactivos, Promedias_entre_compras, \
                Dias_de_Pago, Promedio_dias_de_vencimiento, Dias_factura_vencida_mora, \
                Saldo_Pendiente, Shipping_City, \
                Correo_Electronico_Fact, \
                Transporte_preferido, Shipping_Street, \
                Descripcion, Rating, alert,\
                Shipping_State, \
                Website, Correo_electronico,\
                Phone, Account_Name,ZONA, Referencia, Regimen_Fiscal,RUC) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    updateFormula="UPDATE cliente set entity_id=%s, owner_id=%s,\
                Fecha_Ult_Comp=%s, Limite_de_Credito=%s, Industry=%s, Estado_del_Cliente=%s,\
                Promedio_de_Ventas_Mensual=%s, Dias_Inactivos=%s, Promedias_entre_compras=%s,\
                Dias_de_Pago=%s, Promedio_dias_de_vencimiento=%s, Dias_factura_vencida_mora=%s,\
                Saldo_Pendiente=%s, Shipping_City=%s,\
                Correo_Electronico_Fact=%s,\
                Transporte_preferido=%s, Shipping_Street=%s,\
                Descripcion=%s, Rating=%s, alert=%s,\
                Shipping_State=%s, Website=%s, Correo_electronico=%s,\
                Phone=%s, Account_Name=%s ,ZONA=%s, Referencia=%s, Regimen_Fiscal=%s\
                where RUC=%s"
    
    # Columnas referenciadas de la tabla Cliente 
    campos_cliente = ['entity_id', 'owner_id', 'Fecha_ltima_compra1',\
    'L_mite_de_Cr_dito', 'Industry', 'Estado_del_Cliente',\
    'Promedio_de_Ventas_Mensual', 'D_as_Inactivos',\
    'Promedio_d_as_entre_compras', 'D_as_de_Pago_2',\
    'Promedio_de_D_as_de_vencimiento', 'D_as_factura_vencida_mora',\
    'Saldo_Pendiente', 'Shipping_City', 'Correo_electr_nico_Facturaci_n',\
    'Transporte_preferido', 'Shipping_Street', 'Description', 'Rating','Alerta',\
    'Shipping_State', 'Website', 'Correo_electr_nico', 'Phone', \
    'Account_Name', 'Zona', 'Referencia', 'R_gimen_Fiscal','RUC_CI']
    
    # Credenciales MySQL BD
    conn = mysql.connector.connect(host="localhost",database="cecomex_final",user=user,passwd=password)
    c= conn.cursor()
    
    # Credenciales ZohoCRM
    configuration_dictionary = { 
    "sandbox":"false",
    "applicationLogFilePath":"./logins",                                    # Registro de ingresos a ZohoCRM
    "currentUserEmail": "jefe.proyectos@cecomex.com.ec",
    "client_id": "1000.YVMNLP5AO3Q8TK6V0KI0H62P5X22QH",
    "client_secret": "b3fc31536fb55e41140e7ddf1b38eecf8172fa611b",
    "redirect_uri": "http://www.zoho.com",
    "token_persistence_path": "./TokenPersistence"                          # Token de persistencia que permite generar un nuevo token de acceso a ZohoCRM
    }

    # Extraccion de datos (Accounts) de ZohoCRM
    ZCRMRestClient.initialize(configuration_dictionary)
    module_ins = ZCRMModule.get_instance('Accounts')  
    #resp = module_ins.get_records()
    
    # Iterar por los registros de ZohoCRM(Accounts) clientes
    for i in range(0,1000):
        try:
            resp = module_ins.get_records(page=i)                           # Seleccionar todas las paginas del CRM
            for record_ins in resp.data:                                    # Iterar por pagina
                cliente = (record_ins.entity_id, record_ins.owner.id)       # ID de la entidad y propietario
                product = []                                                # Lista temporal
                product_data = record_ins.field_data                        # Tomar los campos de ZohoCRM (Account)
                for column in campos_cliente:                               # Extraer campos de cliente (Account)
                    value = 'null'
                    if column != 'entity_id' and column != 'owner_id':
                        for key in product_data:
                            if key == column:
                                value = product_data[key]
                        product.append(str(value))
                cliente = cliente + tuple(product)        
                try:
                    if 'prueba' in product_data['Account_Name']:
                        print(updateFormula%cliente)
                    if product_data['RUC_CI']=='1712635232':
                        print(product_data)
                    if product_data['RUC_CI'] in ruc:
                        c.execute(updateFormula,cliente)
                        #print(updateFormula%cliente)
                    else:
                        ruc.append(product_data['RUC_CI'])
                        c.execute(insertFormula,cliente)
                        print(insertFormula%cliente)
                    conn.commit()
                except:
                    print(product_data['Account_Name'])
        except:
            print('pages %s'%i)
            break
            #else:
                #try:
                    #c.execute(insertFormula%cliente)
               # except:
                    #c.execute(insertFormula%cliente)
        #except:
            #print(cliente)
            #print(insertFormula%cliente)
            #print(product_data)

'''
record=ZCRMRecord.get_instance('Accounts',4391497000000711014)#410888000000698006 is leadid
record.set_field_value('Account_Name', 'ZAMORA CANTOS ADALBERTO')
record.set_field_value('Mobile', '9999999999')
record.set_field_value('Phone', '9999999998')
user=ZCRMUser.get_instance(1386586000000105001,'Python User1')
record.set_field_value('Email', 'support@zohocrm.com')
record.set_field_value('Owner',user)
resp=record.update()
 #Create Table
    
CREATE TABLE cliente (entity_id VARCHAR(255), owner_id VARCHAR(255),                     Fecha_Ult_Comp VARCHAR(255), Limite_de_Credito VARCHAR(255), Industry VARCHAR(255),                     Estado_del_Cliente VARCHAR(255), Promedio_de_Ventas_Mensual VARCHAR(255),                     Dias_Inactivos VARCHAR(255), Promedias_entre_compras VARCHAR(255),                     Dias_de_Pago VARCHAR(255), Promedio_dias_de_vencimiento VARCHAR(255),                     Dias_factura_vencida_mora VARCHAR(255), Saldo_Pendiente VARCHAR(255),                     Shipping_City VARCHAR(255), Correo_Electronico_Fact VARCHAR(255),                    Transporte_preferido VARCHAR(255), Shipping_Street VARCHAR(255),                     Descripcion VARCHAR(255),                     Rating VARCHAR(255), alert VARCHAR(255),Shipping_State VARCHAR(255), Website VARCHAR(255),                    Correo_electronico VARCHAR(255),Phone VARCHAR(255), Account_Name VARCHAR(255), RUC VARCHAR(255),
                    Regimen_Fiscal VARCHAR(255), ZONA VARCHAR(255), Referencia VARCHAR(255))


'''
