from db.querys_db import consult_data
from db.conexion_db import cerrar_conexion
import pandas as pd
import numpy as np

def reservas_avisorden_ordenes(connection):

    try:

        pl_reservas = "plan_reservas"
        pl_avisorden = "plan_avisorden"
        pl_ordenes = "plan_ordenes"
        
        reservas = consult_data(connection,f"select * from {pl_reservas}")

        aviso_orden = consult_data(connection,f"select * from {pl_avisorden}")

        ordenes = consult_data(connection,f"select orden,claot,equipo,material,felib,orden_texto,orden_estsis,reservasolped,orden_prio,orden_prio_texto,orden_revision,estatususuarioorden from {pl_ordenes}")
        
        material = consult_data(connection,'select material,Cod_mate_antiguo,Grp_mat,Tp_mat,Jerarquia_produc from tbl_materialesIH09')

        tabla_reservas = pd.DataFrame(reservas)
        tabla_ordenes = pd.DataFrame(ordenes)
        tabla_aviso = pd.DataFrame(aviso_orden)
        tabla_material = pd.DataFrame(material)

        tabla_aviso.drop_duplicates(['orden'],inplace=True)
        tabla_ordenes.drop_duplicates(['orden'],inplace=True)

        tabla_ordenes.rename(columns={"material": "modelo", "orden_prio": "prio_orden","orden_prio_texto" :"prio_orden_texto"},inplace=True)
        tabla_aviso.drop(columns=['id','fechacarga'],inplace=True)
        tabla_reservas.drop(columns=['id','fechacarga'],inplace=True)

        filtrar_tbl_reservas = tabla_reservas.loc[(tabla_reservas['salida_fin']!='X') & (tabla_reservas['reserva_pos_borrado']!='X') & (tabla_reservas['relevplnec']!='X') & (tabla_reservas['orden']!='')].reset_index(drop=True)

        unir_reservas_ordenes = filtrar_tbl_reservas.merge(tabla_ordenes,how='left',on='orden')

        unir_reservaxorden_aviso = unir_reservas_ordenes.merge(tabla_aviso,how='left',on='orden')
        unir_reservaxorden_aviso.where(pd.notna(unir_reservaxorden_aviso),None,inplace=True)

        unir_reservaxorden_aviso['felib'] = np.where( unir_reservaxorden_aviso['felib_x'].isnull(), unir_reservaxorden_aviso['felib_y']  , unir_reservaxorden_aviso['felib_x'])
        unir_reservaxorden_aviso['equipo'] = np.where( unir_reservaxorden_aviso['equipo_x'].isnull(), unir_reservaxorden_aviso['equipo_y']  , unir_reservaxorden_aviso['equipo_x'])
        unir_reservaxorden_aviso['orden_texto'] = np.where( unir_reservaxorden_aviso['orden_texto_x'].isnull(), unir_reservaxorden_aviso['orden_texto_y']  , unir_reservaxorden_aviso['orden_texto_x'])
        unir_reservaxorden_aviso['orden_revision'] = np.where( unir_reservaxorden_aviso['orden_revision_x'].isnull(), unir_reservaxorden_aviso['orden_revision_y']  , unir_reservaxorden_aviso['orden_revision_x'])
        unir_reservaxorden_aviso['estatususuarioorden'] = unir_reservaxorden_aviso['estatususuarioorden_x']
        unir_reservaxorden_aviso['detalle01'] = unir_reservaxorden_aviso['estatususuarioorden_y']
        unir_reservaxorden_aviso['detalle02'] = unir_reservaxorden_aviso['claot']

        unir_reservaxorden_aviso.drop(columns='felib_x', inplace=True)
        unir_reservaxorden_aviso.drop(columns='felib_y', inplace=True)
        unir_reservaxorden_aviso.drop(columns='equipo_x', inplace=True)
        unir_reservaxorden_aviso.drop(columns='equipo_y', inplace=True)
        unir_reservaxorden_aviso.drop(columns='orden_texto_x', inplace=True)
        unir_reservaxorden_aviso.drop(columns='orden_texto_y', inplace=True)
        unir_reservaxorden_aviso.drop(columns='orden_revision_x', inplace=True)
        unir_reservaxorden_aviso.drop(columns='orden_revision_y', inplace=True)

        unir_reservaxorden_aviso['ate_accion'] = ""
        unir_reservaxorden_aviso['ate_orden'] = 999
        unir_reservaxorden_aviso['valor_flota'] = 0.00
        unir_reservaxorden_aviso['stock_asignado'] = 0.00
        unir_reservaxorden_aviso['trasl_ctd_c154'] = 0.00
        unir_reservaxorden_aviso['trasl_ctd_c040'] = 0.00
        unir_reservaxorden_aviso['trasl_ctd_c080'] = 0.00
        unir_reservaxorden_aviso['trasl_ctd_c200'] = 0.00
        unir_reservaxorden_aviso['trasl_ctd_c152'] = 0.00
        unir_reservaxorden_aviso['trasl_fecha'] = None
        unir_reservaxorden_aviso['Ped_nro'] = ''
        unir_reservaxorden_aviso['Ped_Fecha'] = None
        unir_reservaxorden_aviso['Ped_ctd'] = 0.00
        unir_reservaxorden_aviso['Ped_reserva'] = ''
        unir_reservaxorden_aviso['Ped_nro_necesidad'] = ''
        unir_reservaxorden_aviso['Ped_proveedor'] = ''
        unir_reservaxorden_aviso['estadoreserva'] = ''

        unir_reservaxorden_aviso[['ctd_dif','eqvaloradq_usd']] = unir_reservaxorden_aviso[['ctd_dif','eqvaloradq_usd']].astype(float)
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['eqvaloradq_usd'].isna(),'eqvaloradq_usd'] = 1

        unir_reservaxorden_aviso['estadoreserva'] = np.where( (unir_reservaxorden_aviso['orden'] != "") & (unir_reservaxorden_aviso['ot_liberado'] == 'X') & (unir_reservaxorden_aviso['reserva_pos_borrado'] != 'X') ,"Lib Mov.Permitdo", unir_reservaxorden_aviso['estadoreserva'])
        unir_reservaxorden_aviso['estadoreserva'] = np.where( (unir_reservaxorden_aviso['orden'] != "") & (unir_reservaxorden_aviso['ot_liberado'] == '') & (unir_reservaxorden_aviso['reservasolped'] == '3') & (unir_reservaxorden_aviso['reserva_pos_borrado'] != 'X') ,"Aprob Mov.Permitdo", unir_reservaxorden_aviso['estadoreserva'])
        unir_reservaxorden_aviso['estadoreserva'] = np.where( (unir_reservaxorden_aviso['orden'] == "") & (unir_reservaxorden_aviso['reserva_pos_borrado'] != 'X') ,"Lib Mov.Permitdo", unir_reservaxorden_aviso['estadoreserva'])

        # DETERMINAR ACCION de Abastecimiento
        # 1. Revisamos los Servicios
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['um_base'] == "ZZ", 'ate_accion'] = "SERVICIO"
        # 2. Revisamos las Reparaciones
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['lote'] == "REPARADO", 'ate_accion'] = "REPARADO"
        # 3. Revisamos la Mercaderia
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['ate_accion'] == "", 'ate_accion'] = "MERCADERIA"
        # 4. Anulamos los que no se deben atender: GARANTIA ubicado en el campo LOTE/Reservas Borradas/Reservas Atendidas o con Orden Cerrada campo salida_fin
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['salida_fin'] == "X" , 'ate_accion'] = "FIN"
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['reserva_pos_borrado'] == "X", 'ate_accion'] = "FIN"
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['lote'] == "GARANTIA" , 'ate_accion'] = "FIN"
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['reservasolped'] == "2" , 'ate_accion'] = "FIN"

        # Personas -> EPPs y Uniformes
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['orden'].str[0] =='P','ate_orden'] = 100

        # STC

        # Equipos -> Ordenes de trabajo Libres o Aprovisionadas con Programa Semanal
        # OCO -> 4er Todas las Ordenes Liberadas o con Aprovisionamiento, con Numero de semana, de Campo
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') , 250, unir_reservaxorden_aviso['ate_orden'])
        # INO -> 1er Orden Los Inoperativos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "I"), 210, unir_reservaxorden_aviso['ate_orden'])
        # MAN -> 2er Orden Los Mantenimientos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 220, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento Preventivo') & (unir_reservaxorden_aviso['orden_texto'].str[0] == 'M'), 220, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento SSGG') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 220, unir_reservaxorden_aviso['ate_orden'])
        # OCO -> 3er Orden Los Operativos con Observaciones
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "O"), 240, unir_reservaxorden_aviso['ate_orden'])

        # Equipos -> Ordenes de trabajo Libres o Aprovisionadas sin Programa Semanal
        # OCO -> 4er Todas las Ordenes Liberadas o con Aprovisionamiento, con Numero de semana, de Campo
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') , 300, unir_reservaxorden_aviso['ate_orden'])
        # INO -> 1er Orden Los Inoperativos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "I"), 260, unir_reservaxorden_aviso['ate_orden'])
        # MAN -> 2er Orden Los Mantenimientos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 270, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento Preventivo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 270, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento SSGG') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 270, unir_reservaxorden_aviso['ate_orden'])
        # OCO -> 3er Orden Los Operativos con Observaciones
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "X" ) | ((unir_reservaxorden_aviso['reservasolped'] == "3" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "O"), 290, unir_reservaxorden_aviso['ate_orden'])

        # Equipos -> Ordenes de trabajo NO Libres o NO Aprovisionadas con Programa Semanal
        # OCO -> 4er Todas las Ordenes Liberadas o con Aprovisionamiento, con Numero de semana, de Campo
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') , 950, unir_reservaxorden_aviso['ate_orden'])
        # INO -> 1er Orden Los Inoperativos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "I"), 910, unir_reservaxorden_aviso['ate_orden'])
        # MAN -> 2er Orden Los Mantenimientos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 920, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento Preventivo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 920, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento SSGG') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 920, unir_reservaxorden_aviso['ate_orden'])
        # OCO -> 3er Orden Los Operativos con Observaciones
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "O"), 940, unir_reservaxorden_aviso['ate_orden'])

        # Equipos -> Ordenes de trabajo NO Libres o NO Aprovisionadas sin Programa Semanal
        # OCO -> 4er Todas las Ordenes Liberadas o con Aprovisionamiento, con Numero de semana, de Campo
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') , 990, unir_reservaxorden_aviso['ate_orden'])
        # INO -> 1er Orden Los Inoperativos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "I"), 960, unir_reservaxorden_aviso['ate_orden'])
        # MAN -> 2er Orden Los Mantenimientos
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 970, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento Preventivo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 970, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Mantenimiento SSGG') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "M"), 970, unir_reservaxorden_aviso['ate_orden'])
        # OCO -> 3er Orden Los Operativos con Observaciones
        unir_reservaxorden_aviso['ate_orden'] = np.where( ((unir_reservaxorden_aviso['ot_liberado'] == "" ) & ((unir_reservaxorden_aviso['reservasolped'] == "" ))) & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Campo') & (unir_reservaxorden_aviso['orden_texto'].str[0]== "O"), 980, unir_reservaxorden_aviso['ate_orden'])

        # Agrupar por Valor de Flota
        lista_criterio1 = list(unir_reservaxorden_aviso["proyecto"])
        lista_criterio2 = list(unir_reservaxorden_aviso["ate_orden"])

        unir_reservaxorden_aviso['eqvaloradq_usd'] = unir_reservaxorden_aviso['eqvaloradq_usd'].replace('' , 0)

        valor_flota = [unir_reservaxorden_aviso.loc[(unir_reservaxorden_aviso['proyecto'] == lista_criterio1[i]) & (unir_reservaxorden_aviso['ate_orden'] ==lista_criterio2[i]),"eqvaloradq_usd"].sum() for i in range(unir_reservaxorden_aviso.shape[0])]
        # valor_flota.format("{:.2f}")
        valor_flota = pd.DataFrame(valor_flota)
        unir_reservaxorden_aviso["valor_flota"] = valor_flota

        # STT
        # ---

        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "")  & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller'), 999, unir_reservaxorden_aviso['ate_orden'])
        # Equipos -> Ordenes de trabajo Libres con Programa Semanal
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Reparación"), 230, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Preparación"), 230, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Reparación venta"), 230, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Alistamiento"), 240, unir_reservaxorden_aviso['ate_orden'])

        # Equipos -> Ordenes de trabajo Libres sin Programa Semanal
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Reparación"), 280, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Preparación"), 280, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Reparación venta"), 280, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "X")  & (~(unir_reservaxorden_aviso['orden_revision'].str[0]== "C")) & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Alistamiento"), 290, unir_reservaxorden_aviso['ate_orden'])

        # Equipos -> Ordenes de trabajo NO Libres
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Reparación"), 990, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Preparación"), 990, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Reparación venta"), 990, unir_reservaxorden_aviso['ate_orden'])
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['ot_liberado'] == "")  & (unir_reservaxorden_aviso['orden_revision'].str[0]== "C") & (unir_reservaxorden_aviso['oden_clase_deno'] == 'Reparación Taller') & (unir_reservaxorden_aviso['orden_claact_deno'] == "Alistamiento"), 990, unir_reservaxorden_aviso['ate_orden'])

        # 2.3 Equipamiento y Otros

        # Reservas para Centro de Costos
        unir_reservaxorden_aviso['ate_orden'] = np.where( (unir_reservaxorden_aviso['orden'] == ''), 500, unir_reservaxorden_aviso['ate_orden'])
        # Ordenes de Proyecto: 1 (113xxxx -> Proyectos de Mejora / 114xxxxx -> Compra de Activos / 18xxx -> Proyectos Campo) 
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['orden'].str[0] == "1" , 'ate_orden'] = 600
        # Ordenes de Proyecto: 2
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['orden'].str[0] == "2" , 'ate_orden'] = 620
        # Ordenes de Capacitacion: 7
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['orden'].str[0] == "7" , 'ate_orden'] = 640

        # Las reservas que no se vana utilizar (ate_accion = "FIN"), colocar 999
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['ate_accion'] == "FIN" , 'ate_orden'] = 999
        unir_reservaxorden_aviso.loc[unir_reservaxorden_aviso['ate_orden'] == 999 , 'ate_accion'] = "FIN"

        unir_reservaxorden_aviso['material'] = unir_reservaxorden_aviso['material'].str.replace(' ','')
        tabla_material['material'] = tabla_material['material'].str.replace(' ','')

        reservaxorden_material = unir_reservaxorden_aviso.merge(tabla_material,how='left',on='material')
        reservaxorden_material['identificador']=(reservaxorden_material['orden']+reservaxorden_material['reserva']+reservaxorden_material['reservapos'])
        reservaxorden_material = reservaxorden_material.sort_values(by = ['ate_orden', 'valor_flota','fe_nece', 'reserva', 'reservapos'], ascending = [True, False, True, True, True], na_position = 'last').reset_index(drop=True)
        reservaxorden_material['index'] = reservaxorden_material.index

        return reservaxorden_material
    
    except Exception as e:

        print(f"Error al ejecutar el procedimiento cruce_inventario: {e}")
        return []

    finally:
        cerrar_conexion(connection)

