import mysql.connector as mysql
from mysql.connector import Error
import json
import os

def  get_credenciales(path = "db/credenciales_db.json"):
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    credenciales_path = os.path.join(base_dir, path)

    with open(credenciales_path, "r") as config_file:
        return json.load(config_file)
    
def crear_conexion():
    try:
        credenciales = get_credenciales()
        conn = mysql.connect(
            host=credenciales['host'],
            user=credenciales['user'],
            password=credenciales['password'],
            database=credenciales['database']
        )
        if conn.is_connected():
            print("Conexion Exitosa")
            return conn
    except Error as e:
        print("Error al conectar a la base de datos:",e )
        return None

def cerrar_conexion(conn):
    if conn.is_connected():
        conn.close()
        print("conexion cerrada")
