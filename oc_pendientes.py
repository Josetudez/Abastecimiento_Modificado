from db.querys_db import consult_data,procedure_data,insert_data
from db.conexion_db import cerrar_conexion
import pandas as pd
import numpy as np

def pedidospendiente(pedidospendiente,trazabilidad):
    try:
        tbl_pedidos_pendiente = pd.DataFrame(pedidospendiente)
        tbl_trazabalidad_Compras = pd.DataFrame(trazabilidad)
        tbl_pedidos_pendiente['identificador'] = tbl_pedidos_pendiente['doc_compra'] + tbl_pedidos_pendiente['pos']
        tbl_trazabalidad_Compras['identificador'] = tbl_trazabalidad_Compras['Doc_compra'] + tbl_trazabalidad_Compras['Pos_doc_cmp']
        
        pedidos_trazabilidad= tbl_pedidos_pendiente.merge(tbl_trazabalidad_Compras,how='left',on='identificador')

        pedidos_trazabilidad.drop(columns='Doc_compra',inplace=True)
        pedidos_trazabilidad.drop(columns='Pos_doc_cmp',inplace=True)
        pedidos_trazabilidad.drop(columns='id',inplace=True)
        pedidos_trazabilidad.drop(columns='fechacarga',inplace=True)

        filtrar_pedidosxtrazabilidad = pedidos_trazabilidad.loc[pedidos_trazabilidad['Estado'].isin(['APROBADO','PENDIENTE DE APROBACIÓN'])]
        filtrar_pedidosxtrazabilidad.loc[:, 'Hora'] = pd.to_timedelta(filtrar_pedidosxtrazabilidad['Hora']).apply(lambda x: str(x).split()[2])

        return filtrar_pedidosxtrazabilidad

    except Exception as e:

        print(f"Error al ejecutar el procedimiento pedidospendientes: {e}")
        return[]

    