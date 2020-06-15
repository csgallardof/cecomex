from zcrmsdk import ZCRMRecord ,ZCRMRestClient, ZCRMModule, ZohoOAuth
import mysql.connector

#Create Connection
def start_connection():
    configuration_dictionary = { 
        "sandbox":"false",
        "currentUserEmail": "jefe.proyectos@cecomex.com",
        "client_id": "1000.YVMNLP5AO3Q8TK6V0KI0H62P5X22QH",
        "client_secret": "b3fc31536fb55e41140e7ddf1b38eecf8172fa611b",
        "redirect_uri": "http://www.zoho.com",
        "token_persistence_path": "./TokenPersistence"
    }

    ZCRMRestClient.initialize(configuration_dictionary) 

    oauth_client = ZohoOAuth.get_client_instance()  
    grant_token="1000.02035858ddbe40a064547398d3eca6ec.dea2131577187ef68129a4159efea9e3" 
    oauth_tokens = oauth_client.generate_access_token(grant_token) 
    print(oauth_tokens.__dict__)

    #Scope: AAAServer.profile.read,ZohoCRM.Modules.All,ZohoCRM.Bulk.READ,ZohoCRM.org.all
    #Modules Scope: ZohoCRM.Modules.All


#Create DB
def create_db(db_name, db_host, db_user, db_pass):
    mydb = mysql.connector.connect(
        host = db_host,
        user = db_user,
        password = db_pass,
    )

    mycursor = mydb.cursor()

    mycursor.execute("CREATE DATABASE " + db_name)

    #Updata connection
    mydb = mysql.connector.connect(
        host = db_host,
        user = db_user,
        password = db_pass,
        database = db_name)

    mycursor = mydb.cursor()

    #Show DBs
    mycursor.execute("SHOW DATABASES")
    for db in mycursor:
        print(db)

    #Create Table
    mycursor.execute("CREATE TABLE cliente (Creado_por VARCHAR(255), \
                    Propietario_de_Cliente VARCHAR(255), Modificado_por VARCHAR(255), \
                    Fecha_Ultima_Compra VARCHAR(255), Limite_de_Credito VARCHAR(255), Sector VARCHAR(255), \
                    Estado_del_Cliente VARCHAR(255), Promedio_de_Ventas_Mensual VARCHAR(255), \
                    Dias_Inactivos VARCHAR(255), Promedias_dias_entre_compras VARCHAR(255), \
                    Dias_de_Pago VARCHAR(255), Promedio_de_dias_de_vencimiento VARCHAR(255), \
                    Dias_factura_vencida_mora VARCHAR(255), Saldo_Pendiente VARCHAR(255), \
                    Ciudad_de_envio VARCHAR(255), Correo_electronico_facturacion VARCHAR(255),\
                    Transporte_preferido VARCHAR(255), Domicilio_de_envio VARCHAR(255), \
                    Descripcion VARCHAR(255),\
                    Calificacion VARCHAR(255), Estado_de_envio VARCHAR(255), Sitio_web VARCHAR(255),\
                    Correo_electronico VARCHAR(255),Telefono VARCHAR(255), \
                    Nombre_de_Cliente VARCHAR(255), RUC VARCHAR(255),\
                    Alerta VARCHAR(255), Linea_de_Negocio VARCHAR(255))")

    #Show Tables
    mycursor.execute("SHOW TABLES")
    for tb in mycursor:
        print(tb)


#Consult and Insert
def consult_insert(db_name, db_host, db_user, db_pass): 
    configuration_dictionary = { 
    "sandbox":"false",
    "applicationLogFilePath":"./logins",
    "currentUserEmail": "jefe.proyectos@cecomex.com.ec",
    "client_id": "1000.YVMNLP5AO3Q8TK6V0KI0H62P5X22QH",
    "client_secret": "b3fc31536fb55e41140e7ddf1b38eecf8172fa611b",
    "redirect_uri": "http://www.zoho.com",
    "token_persistence_path": "./TokenPersistence"
    }

    mydb = mysql.connector.connect(
        host = db_host,
        user = db_user,
        password = db_pass,
        database = db_name)

    ZCRMRestClient.initialize(configuration_dictionary)
    module_ins = ZCRMModule.get_instance('Accounts')  
    resp = module_ins.get_records()
    #print(resp.status_code)    #numero de clientes

    mycursor = mydb.cursor()
    sqlFormula = "INSERT INTO cliente (Creado_por, Propietario_de_Cliente, Modificado_por,\
                Fecha_Ultima_Compra, Limite_de_Credito, Sector, Estado_del_Cliente, \
                Promedio_de_Ventas_Mensual, Dias_Inactivos, Promedias_dias_entre_compras, \
                Dias_de_Pago, Promedio_de_dias_de_vencimiento, \
                Dias_factura_vencida_mora, Saldo_Pendiente, Ciudad_de_envio, \
                Correo_electronico_facturacion, \
                Transporte_preferido, Domicilio_de_envio, \
                Descripcion,  Calificacion,\
                Estado_de_envio, \
                Sitio_web, Correo_electronico,\
                Telefono, Nombre_de_Cliente, RUC,\
                Alerta, Linea_de_Negocio) \
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    record_ins_arr = resp.data
    campos_cliente = ['created_by', 'owner_id', 'modified_by', 'Fecha_ltima_compra1', 'L_mite_de_Cr_dito', 'Industry', 'Estado_del_Cliente', 'Promedio_de_Ventas_Mensual', 'D_as_Inactivos', 'Promedio_d_as_entre_compras', 'D_as_de_Pago_2', 'Promedio_de_D_as_de_vencimiento', 'D_as_factura_vencida_mora', 'Saldo_Pendiente', 'Shipping_City', 'Correo_electr_nico_Facturaci_n', 'Transporte_preferidoEdit', 'Shipping_Street', 'Description', 'Rating', 'Shipping_State', 'Website', 'Correo_electr_nico', 'Phone', 'Account_Name', 'RUC', 'Alerta', 'L_nea_de_Negocio']
    print('campos', len(campos_cliente))

    for record_ins in record_ins_arr:
        #print(record_ins.__dict__)
        #print(record_ins.modified_by.id)
        #print(record_ins.created_by.id)
        cliente = (record_ins.entity_id, record_ins.owner.id, record_ins.modified_by.id)
        product = []
        product_data = record_ins.field_data
        for column in campos_cliente:
            value = 'null'
            if column != 'created_by' and column != 'owner_id' and column != 'modified_by':
                for key in product_data:
                    if key == column:
                        value = product_data[key]
                product.append(str(value))
        cliente = cliente + tuple(product)        
        #print(cliente)
        mycursor.execute(sqlFormula, cliente)
        mydb.commit()


if __name__ == "__main__":
    db_name = 'cecomex_final_zasdf'
    host = "127.0.0.1"
    user = "root"
    password = "bike1234567"

    create_db(db_name, host, user, password)
    #start_connection()             #Para iniciar una nueva conexion, crear un grant token en zoho developers.
    consult_insert(db_name, host, user, password)
