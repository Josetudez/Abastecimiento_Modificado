from db.querys_db import consult_data,procedure_data,insert_data
from db.conexion_db import cerrar_conexion
import pandas as pd
import numpy as np

def pedidospendiente(connection):
    try:

        tbl_pedidos_pendientes = 'tbl_pedidospendientes_me2n'
        plan_pedidospendientes= 'plan_pedidospendientes'
        rpt_estado_compras_pendientes = 'rpt_estado_compras_pendientes'
        plan_pedidospendientes_resumen = 'plan_pedidospendientes_resumen'
        plan_trazabilidad = "plan_trazabilidad_compras"

        consult_data(connection,f"truncate table {plan_pedidospendientes}")
        consult_data(connection,f"truncate table {rpt_estado_compras_pendientes}")
        consult_data(connection,f"truncate table {plan_pedidospendientes_resumen}")

        cabezeras_estado_compra = ['id','fechacarga','doc_compra','pos','contr','pos1','orgc','un','rep','cl_documento','tipo_documento','grupo_documento','Hist_ped','Fecha_doc','Nombre_proveedor','Centro_suministrador','Material','txt_breve','Gpo_articulo','I_borrado',
                                'T_posicion','T_imp','Centro','Alm','Cantidad','Por_entr_Ctd','UMP','Precio_neto','Valor_neto','Val_pend_total','Val_pend','Por_entr_valor','Moneda','c_base','ctd_reparto','Fecha_entrega','Hora','Fech_entr_estad','Ctd_ante','Ctd_ent',
                                'Salida','Ctd_entr','Pedido','Pos_pedido','Ind_creacion','Ctd_posicion','Num_necesidad','Fech_solicitud','Fe_pedido','Fe_entr_pre','Fe_doc_en','Estado','Liberador_1','Liberador_2','Liberador_3','Liberador_4','Creador_doc_comp']
    
        
        insert_plan_pedidospendiente = f"insert into {plan_pedidospendientes} select * from {tbl_pedidos_pendientes} where fechacarga = (select max(fechacarga) from {tbl_pedidos_pendientes}) "
        consult_data(connection,insert_plan_pedidospendiente)

        procedure_data(connection,'abastece_plan_trazabilidad')

        tbl_pedidospendiente = consult_data(connection,f"select * from {plan_pedidospendientes}")
        tbl_trazabilidad = consult_data(connection,f"select * from {plan_trazabilidad}")

        pedidos_pendiente = pd.DataFrame(tbl_pedidospendiente)
        trazabalidad_Compras = pd.DataFrame(tbl_trazabilidad)


        trazabilidad_filtrada = trazabalidad_Compras[['Fech_solicitud','Fe_pedido','Fe_entr_pre','Fe_doc_en','Estado','Liberador_1','Liberador_2','Liberador_3','Liberador_4',
                                                                    'Creador_doc_comp','Doc_compra','Pos_doc_cmp']]
        pedidos_pendiente['union'] = pedidos_pendiente['doc_compra']+''+pedidos_pendiente['pos']
        trazabilidad_fil=trazabilidad_filtrada.copy()
        trazabilidad_fil['union'] = trazabilidad_fil['Doc_compra']+''+trazabilidad_fil['Pos_doc_cmp']

        union_pedidos= pedidos_pendiente.merge(trazabilidad_fil,how='left',on='union')
        union_pedidos.drop(columns='union',inplace=True)
        union_pedidos.drop(columns='Doc_compra',inplace=True)
        union_pedidos.drop(columns='Pos_doc_cmp',inplace=True)

        union_pedidos['fechacarga'] = pd.to_datetime(union_pedidos['fechacarga']).dt.strftime('%Y-%m-%d %H:%M:%S')
        union_pedidos['Hora'] = pd.to_timedelta(union_pedidos['Hora']).apply(lambda x: str(x).split()[2])

        cargar_estado_compras_pendientes = union_pedidos.copy()
        
        insert_data(connection,rpt_estado_compras_pendientes,cargar_estado_compras_pendientes,cabezeras_estado_compra,batch_size=1000)

        insert_plan_pedidospendiente_resumen = f"insert into  {plan_pedidospendientes_resumen} select id,doc_compra,pos,material,centro,por_entr_ctd,fecha_entrega,pedido,num_necesidad,nombre_proveedor from {rpt_estado_compras_pendientes} where por_entr_ctd > 0 and estado not like '%RECHA%' "
        
        consult_data(connection,insert_plan_pedidospendiente_resumen)

    except Exception as e:

        print(f"Error al ejecutar el procedimiento pedidospendientes: {e}")
        return[]

    finally:
        cerrar_conexion(connection)