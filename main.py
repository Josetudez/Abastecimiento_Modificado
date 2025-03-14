from db.conexion_db import crear_conexion, cerrar_conexion
from oc_pendientes import pedidospendiente
from cruce_inventario import cruce_inventarios
from abastecimiento import reservas_avisorden_ordenes
from db.querys_db import consult_data, insert_data, procedure_data


from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

def main():

    tbl_pedidos_pendientes = 'tbl_pedidospendientes_me2n'
    plan_pedidospendientes= 'plan_pedidospendientes'
    plan_trazabilidad = "plan_trazabilidad_compras"
    pl_inventarioMB52 = "plan_inventario_resumen"

    cabezeras_estado_compra = ['id','fechacarga','doc_compra','pos','contr','pos1','orgc','un','rep','cl_documento','tipo_documento','grupo_documento','Hist_ped','Fecha_doc','Nombre_proveedor','Centro_suministrador','Material','txt_breve','Gpo_articulo','I_borrado',
                            'T_posicion','T_imp','Centro','Alm','Cantidad','Por_entr_Ctd','UMP','Precio_neto','Valor_neto','Val_pend_total','Val_pend','Por_entr_valor','Moneda','c_base','ctd_reparto','Fecha_entrega','Hora','Fech_entr_estad','Ctd_ante','Ctd_ent',
                            'Salida','Ctd_entr','Pedido','Pos_pedido','Ind_creacion','Ctd_posicion','Num_necesidad','Fech_solicitud','Fe_pedido','Fe_entr_pre','Fe_doc_en','Estado','Liberador_1','Liberador_2','Liberador_3','Liberador_4','Creador_doc_comp']
    
    connection = crear_conexion()
    procedure_data(connection,"abastece_plan_reservas")
    procedure_data(connection,"abastece_plan_avisorden")
    procedure_data(connection,"abastece_plan_ordenes")
    cerrar_conexion(connection)

    connection = crear_conexion()
    procedure_data(connection,"abastece_plan_inv_mb52")
    procedure_data(connection,'abastece_plan_inv_scwm')         
    procedure_data(connection,'abastece_plan_trazabilidad')
    cerrar_conexion(connection)   

    connection = crear_conexion()
    reservas = reservas_avisorden_ordenes(connection)
    cerrar_conexion(connection)

    connection = crear_conexion()
    inventario_cruce = cruce_inventarios(connection)

    connection = crear_conexion()
    consult_data(connection,f"truncate table {plan_pedidospendientes}")
    insert_plan_pedidospendiente = f"insert into {plan_pedidospendientes} select * from {tbl_pedidos_pendientes} where fechacarga = (select max(fechacarga) from {tbl_pedidos_pendientes}) "
    consult_data(connection,insert_plan_pedidospendiente)
    pedidosxpendiente = consult_data(connection,f"select * from {plan_pedidospendientes}")
    trazabilidad = consult_data(connection,f"select Fech_solicitud,Fe_pedido,Fe_entr_pre,Fe_doc_en,Estado,Liberador_1,Liberador_2,Liberador_3,Liberador_4,Creador_doc_comp,Doc_compra,Pos_doc_cmp from {plan_trazabilidad}")
    planinventarioresumen = consult_data(connection,f"select * from {pl_inventarioMB52}")
    cerrar_conexion(connection)

    connection = crear_conexion()
    estado_pedidosxpendiente = pedidospendiente(pedidosxpendiente,trazabilidad)

    tbl_inventario_resumen = pd.DataFrame(planinventarioresumen)

    filtrar_cabezeras_estadoxpedidos = estado_pedidosxpendiente[['identificador','doc_compra','Material','Centro','Por_entr_Ctd','Fecha_entrega','Pedido','Num_necesidad','Nombre_proveedor']]

    

if __name__ == "__main__":
    main()
