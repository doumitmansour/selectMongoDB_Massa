from pymongo import MongoClient
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime, timedelta
import pymongo
from bson import ObjectId
import thinc.util as tu

placas_buscar = pd.read_excel('placas_buscar.xlsx')

def bruto(db,modulo,dt1, dt2):
    dt2 = datetime.strptime(str(dt2), '%Y-%m-%d %H:%M:%S') + timedelta(hours=3)
    dt1 = datetime.strptime(str(dt1), '%Y-%m-%d %H:%M:%S') + timedelta(hours=3)

    colecao = db.pvpackets
    query = {'_s_mid': modulo}
    projection = {'_iddevice': 1, '_id': 0}
    sort = [('_dt_recv', pymongo.DESCENDING)]
    print('1')
    limit = 1
    print('2')
    cursor = colecao.find(query, projection).sort(sort).limit(limit)
    print('3')
    df = pd.json_normalize(cursor)
    print('4')

    iddevice = str(df.at[0,'_iddevice'])
    print('5')
    object_id_iddevice = ObjectId(iddevice)
    print('6')

    colecao = db.pvsignals
    print('7')
    query = { '$and': [{'_iddevice': object_id_iddevice},{'_dt_creation': { '$gte' : dt1, '$lt':dt2 }}]}
    print('8')
    sort = [('_dt_creation',pymongo.ASCENDING)]
    print('9')
    cursor = colecao.find(query).sort(sort)
    print('10')
    df_bruto = pd.json_normalize(cursor)
    print('11')

    return df_bruto

def excesso_velocidade(placa, dh):

        banco = 'RELATORIO'
        query = f"""
        SELECT 
      [placa]
      ,[date] as dt_referencia
      ,[time] as hr_evento
      ,[duration]
      ,distance
      ,CONCAT(date,' ',time) as dh_inicial
	  ,DATEADD(SECOND, DATEDIFF(SECOND, '00:00:00', [duration]), CONCAT([date], ' ', [time])) AS dh_final
      FROM [RELATORIO].[dbo].[rel_uniserede_excesso_velocidade]
      where placa = '{placa}'
      and  date = '{dh}'
      order by time desc"""
      
        df_excesso = tu.sql(query, banco)
        
        return df_excesso
    
resultados_excesso = []

for index, row in placas_buscar.iterrows():
        placa = row['placa']
        dia = row['dia']
        
        # Chame a sua função excesso_velocidade com a placa e a data (dia)
        df_excesso = excesso_velocidade(placa, dia)
        
        resultados_excesso.append(df_excesso)

df_excesso = pd.concat(resultados_excesso, ignore_index=True)

df_excesso['dh_inicial'] = pd.to_datetime(df_excesso['dh_inicial'])
df_excesso['dh_final'] = pd.to_datetime(df_excesso['dh_final'])
    
def placa_modulo(placa):
    banco = 'producao'
    query = f"select MODULO_INSERIDO, SERVIDOR, MODELO_RASTREADOR from producao..int_inventario where placa = '{placa}'"
    resultado = tu.sql(query, banco)
    modulo = resultado.iloc[0]['MODULO_INSERIDO']
    servidor = resultado.iloc[0]['SERVIDOR']
    modelo_rastreador = resultado.iloc[0]['MODELO_RASTREADOR']
    
    if servidor == 'SEREDE':
        servidor = ''
    elif servidor == 'RM':
        servidor = ''
    else:
        print('SERVIDOR NAO ENCONTRADO: ' + servidor + placa)
    
    if modelo_rastreador == 'ST310U':
        modulo = ('00' + modulo)
    
    return modulo, servidor


resultados_inicio_evento = []
resultados_fim_evento = []

for index, row in df_excesso.iterrows():

    modulo_servidor = placa_modulo(row['placa']) # PASSAR PLACA ----------------------------------------------------
    modulo =  modulo_servidor[0]
    servidor =  modulo_servidor[1]

# rm_s = '132.226.160.19'
# serede_s = '144.22.153.197'

    client = MongoClient(servidor,
                         username='',
                         password='',
                         authSource='',
                         authMechanism='')
    
    db = client.pvinova

    # evento inicial
    
    dh_inicial = row['dh_inicial']
        
    df_temporario_1 = bruto(db, modulo, dh_inicial, dh_inicial + timedelta(seconds=2))
        
    resultados_inicio_evento.append(df_temporario_1)
    
    # evento final - ponto sucessor 
  
    dh_final = row['dh_final']
        
    df_temporario_2 = bruto(db, modulo, dh_final, dh_final + timedelta(seconds=2))
  
    resultados_fim_evento.append(df_temporario_2)
  
    
# evento inicial
df_merged_inicio = pd.concat(resultados_inicio_evento, ignore_index=True)
    
df_merged_inicio = df_merged_inicio.drop(columns=["_dt_location", "_dt_recv"])
    
df_merged_inicio['_dt_creation'] = df_merged_inicio['_dt_creation'] - timedelta(hours=3) #TABELA COMPLETA DO EVENTO INICIAL

colunas_desejadas_1 = ['_dt_creation', '_f_hdop', '_i_hdop_status', '_f_speed', '_i_hodm', 'loc']

for coluna in colunas_desejadas_1:
    if coluna not in df_merged_inicio.columns:
        df_merged_inicio[coluna] = None
    
df_merged_inicio = df_merged_inicio.rename(columns={'_f_hdop': '_f_hdop_inicio', '_i_hdop_status': '_i_hdop_status_inicio','loc': 'loc_inicial', '_i_hodm': '_i_hodm_inicial', '_f_speed': '_f_speed_inicial'})                    
     
df_lat_long_inicio = df_merged_inicio[['_dt_creation','_f_hdop_inicio','_i_hdop_status_inicio', '_f_speed_inicial','_i_hodm_inicial','loc_inicial']]
      
 # evento final - ponto sucessor              
df_merged_fim = pd.concat(resultados_fim_evento, ignore_index=True)
    
df_merged_fim = df_merged_fim.drop(columns=["_dt_location", "_dt_recv"])
    
df_merged_fim['_dt_creation'] = df_merged_fim['_dt_creation'] - timedelta(hours=3) #TABELA COMPLETA DO EVENTO INICIAL

colunas_desejadas_2 = ['_dt_creation', '_f_hdop', '_i_hdop_status', '_f_speed', '_i_hodm', 'loc']

for coluna in colunas_desejadas_2:
    if coluna not in df_merged_fim.columns:
        df_merged_fim[coluna] = None
    
df_merged_fim = df_merged_fim.rename(columns={'_f_hdop': '_f_hdop_fim', '_i_hdop_status': '_i_hdop_status_fim','loc': 'loc_fim', '_i_hodm': '_i_hodm_fim', '_f_speed': '_f_speed_fim'})
    
df_lat_long_fim = df_merged_fim[['_dt_creation','_f_hdop_fim','_i_hdop_status_fim', '_f_speed_fim','_i_hodm_fim','loc_fim']]

# FAZER O JOIN COM O EXCESSO PARA DEIXAR AS INFO EM UM DF SÓ

# Realiza o join usando a coluna 'dh_inicial' como chave
df_resultado = df_excesso.merge(df_lat_long_inicio, left_on='dh_inicial', right_on='_dt_creation', how='inner')
df_resultado = df_resultado.merge(df_lat_long_fim, left_on='dh_final', right_on='_dt_creation', how='inner')
df_resultado = df_resultado.drop(columns=["_dt_creation_x", "_dt_creation_y"])








        
        
    