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
        inventario_ewm = consult_data(connection,f"select * from {tbl_inventario_ewm}")

        inventario_mb = pd.DataFrame(inventario_mb52)
        inventario_sc = pd.DataFrame(inventario_ewm)

        inventario_sc_filtrado = inventario_sc[['Producto','Propietario','Lote','Tipo_stock','ctd_producto']]
        inventario_sc_fil = inventario_sc_filtrado.copy()

        filtrar_inventario_mb = inventario_mb.loc[inventario_mb['alm'].isin(['0002','0012'])].reset_index()
        agrupar_inventario_mb = filtrar_inventario_mb.groupby(['material','materialtxt','centro','tipomaterial','grupoarticulo'])['cantidad'].sum().reset_index()
        agrupar_inventario_mb['union'] = agrupar_inventario_mb['material'] +''+agrupar_inventario_mb['centro']


        filtrar_inventario_sc = inventario_sc_fil.loc[inventario_sc_fil['Tipo_stock'].isin(['S2','M2'])].reset_index()
        pivotear_inventario_sc = filtrar_inventario_sc.pivot_table(index=['Producto','Propietario'],columns='Lote',values='ctd_producto', aggfunc='sum',fill_value=0)
        inv_sc = pivotear_inventario_sc.reset_index()
        inv_sc.rename(columns={inv_sc.columns[3]:'Sin_Asignacion'},inplace=True)
        inv_sc['union'] = inv_sc['Producto']+''+inv_sc['Propietario']


        union_inventarios = agrupar_inventario_mb.merge(inv_sc, how="left", on="union")
        reorganizar_inv = union_inventarios.reindex(['union','material','materialtxt','centro','tipomaterial','grupoarticulo','cantidad','Producto','propietario','Sin_Asignacion','GARAN','NUEVO','REPAR'], axis=1)

        reorganizar_inv[['Sin_Asignacion','GARAN','NUEVO','REPAR','cantidad']] = reorganizar_inv[['Sin_Asignacion','GARAN','NUEVO','REPAR','cantidad']].replace({np.nan: 0})
        
        reorganizar_inv['total_2'] = reorganizar_inv['Sin_Asignacion'].astype(float)+ reorganizar_inv['GARAN'].astype(float)+ reorganizar_inv['NUEVO'].astype(float)+reorganizar_inv['REPAR'].astype(float)
        diferencia_stock = reorganizar_inv.copy()

        diferencia_stock['dif_stock'] = diferencia_stock['cantidad'].astype(float) - diferencia_stock['total_2'].astype(float)
        dif_inv = diferencia_stock.copy()

        dif_inv = dif_inv.replace({np.nan : None})

        insert_data(connection,tbl_cruce_inventario,dif_inv,cabezeras_cruce_inventario,batch_size=1000)

        return dif_inv
    
    except Exception as e:

        print(f"Error al ejecutar el procedimiento cruce_inventario: {e}")
        return []

    finally:
        cerrar_conexion(connection)