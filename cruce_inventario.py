from db.querys_db import consult_data,insert_data
from db.conexion_db import cerrar_conexion
import pandas as pd
import numpy as np

def cruce_inventarios(connection):

    try:

        tbl_inventario_mb52 = 'plan_inventario_mb52'
        tbl_inventario_ewm = 'plan_inventario_scwm'
        tbl_cruce_inventario = 'cruce_inventario'

        cabezeras_cruce_inventario = ['union','material','materialtxt','centro','tipomaterial','grupoarticulo','ctd_total_mb52','Producto','propietario','Sin_Asignacion','Ctd_Garantia','Ctd_nuevo','Ctd_reparacion','ctd_total_scwm','diferencia_stocks']

        consult_data(connection,f"truncate table {tbl_cruce_inventario}")

        inventario_mb52 = consult_data(connection,f"select * from {tbl_inventario_mb52}")
        inventario_ewm = consult_data(connection,f"select Producto,Propietario,Lote,Tipo_stock,ctd_producto from {tbl_inventario_ewm}")

        inventario_mb = pd.DataFrame(inventario_mb52)
        inventario_sc = pd.DataFrame(inventario_ewm)

        filtrar_inventario_mb = inventario_mb.loc[inventario_mb['alm'].isin(['0002','0012'])].reset_index(drop='index')
        agrupar_inventario_mb = filtrar_inventario_mb.groupby(['material','materialtxt','centro','tipomaterial','grupoarticulo'])['cantidad'].sum().reset_index()
        agrupar_inventario_mb['identificador'] = agrupar_inventario_mb['material'] + agrupar_inventario_mb['centro']

        filtrar_inventario_sc = inventario_sc.loc[inventario_sc['Tipo_stock'].isin(['S2','M2'])].reset_index()
        filtrar_inventario_sc['ctd_producto'] = filtrar_inventario_sc['ctd_producto'].astype(float)
        filtrar_inventario_sc.loc[filtrar_inventario_sc['Lote'].isnull() ,'Lote'] = 'Sin lote'
        pivotear_inventario_sc = filtrar_inventario_sc.pivot_table(index=['Producto','Propietario'],columns='Lote',values='ctd_producto', aggfunc='sum', fill_value=0)
        inv_sc = pivotear_inventario_sc.reset_index()
        inv_sc['identificador'] = inv_sc['Producto'] + inv_sc['Propietario']
        union_inventarios = agrupar_inventario_mb.merge(inv_sc, how="left", on="identificador")
        reorganizar_inv = union_inventarios.reindex(['identificador','material','materialtxt','centro','tipomaterial','grupoarticulo','cantidad','Producto','Propietario','Sin lote','GARAN','NUEVO','REPAR'], axis=1)
        reorganizar_inv['total_2'] = reorganizar_inv['Sin lote'] + reorganizar_inv['GARAN'] + reorganizar_inv['NUEVO'] + reorganizar_inv['REPAR']
        reorganizar_inv['dif_stock'] = reorganizar_inv['cantidad'].astype(float) - reorganizar_inv['total_2']

        return reorganizar_inv
    
    except Exception as e:

        print(f"Error al ejecutar el procedimiento cruce_inventario: {e}")
        return []

    finally:
        cerrar_conexion(connection)