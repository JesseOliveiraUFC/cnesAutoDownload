import pandas as pd
import wget
from tqdm import tqdm
from zipfile import ZipFile
import os
import psycopg2
from psycopg2 import Error

competencia = 202206 #É Necessário Digitar a competência que deseja buscar.
data = []


#Cria a pasta com a competência deseja, se a mesma ainda não existir.
try:
    os.mkdir(f"{competencia}")
except OSError:
    print(f"O diretório {competencia} já existe")


#Define os caminhos onde o arquivo será buscado, o nome do arquivo específico
#dentro do arquivo .zip com todos os dados que é baixado do cnes e define o 
#local onde os dados vão ser salvo (a pasta que foi criada anteriormente).
link_down = f"ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_{competencia}.ZIP"
arquivo = f"BASE_DE_DADOS_CNES_{competencia}.ZIP"
save_dir = f"{competencia}/"


#Faz o download dos dados, cajo já não tenha sido baixados.
def DownloadData():
    if os.listdir().count(save_dir+arquivo) == 0:
        print("Baixando os dados da competência solicitada")
        tqdm(wget.download(link_down, save_dir))
        print("Download efetuado com sucesso")
    else:
        print(f" O arquivo {arquivo} já foi baixado")

DownloadData()


#Extrai somente o arquivo que contem informações sobre os estabelecimentos
with ZipFile(save_dir+arquivo, 'r') as zip:
    zip.extract(f"tbEstabelecimento{competencia}.csv", path=save_dir) 


#Faz a leitura dos dados para um Data Frame (data) para realizar o tratamento
#dos dados.
def ReadAndSaveData():
    global data
    #Leitura dos dados
    data = pd.read_csv(save_dir+f"tbEstabelecimento{competencia}.csv", 
                       delimiter = ";", low_memory=False)
    
    #Eliminio todos os dados que não são do Ceará, ou seja, aqueles em que o 
    #CO_ESTADO_GESTOR são diferentes de 23. O Código do Ceará é 23.
    data = data.loc[(data['CO_ESTADO_GESTOR'] == 23)]
    
    #Eliminar as colunas que possuem dados desnecessários
    data = data.drop(columns=['CO_REGIAO_SAUDE',
                       'NU_FAX','TP_PFPJ','NIVEL_DEP',
                       'CO_MICRO_REGIAO','CO_TURNO_ATENDIMENTO',
                       'CO_DISTRITO_SANITARIO',
                       'CO_DISTRITO_ADMINISTRATIVO',
                       'CO_ATIVIDADE','CO_CLIENTELA',
                       'NU_ALVARA','DT_EXPEDICAO',
                       'TP_ORGAO_EXPEDIDOR',
                       'DT_VAL_LIC_SANI',
                       'TP_LIC_SANI',
                       'TO_CHAR(DT_ATUALIZACAO,\'DD/MM/YYYY\')',
                       'CO_USUARIO','CO_CPFDIRETORCLN',
                       'REG_DIRETORCLN','ST_ADESAO_FILANTROP',
                       'CO_MOTIVO_DESAB','NO_URL',
                       'TO_CHAR(DT_ATU_GEO,\'DD/MM/YYYY\')',
                       'NO_USUARIO_GEO','CO_NATUREZA_JUR',
                       'TP_ESTAB_SEMPRE_ABERTO',
                       'ST_GERACREDITO_GERENTE_SGIF',
                       'ST_CONEXAO_INTERNET','CO_ESTADO_GESTOR',
                       'CO_TIPO_UNIDADE','NO_FANTASIA_ABREV',
                       'TP_GESTAO',
                       'TO_CHAR(DT_ATUALIZACAO_ORIGEM,\'DD/MM/YYYY\')',
                       'ST_CONTRATO_FORMALIZADO'
                       ]) 
    
    data = data.astype(str) #Converto o Data Frame para dados tipo STRING
    #Salva os dados em um arquivo .csv local
    data.to_csv(save_dir+f"Estab-{competencia}.csv", index=False)
    
ReadAndSaveData()


#Realizar a conexão com o banco de dados
def Connect():
    global data
    print("Conectando ao Banco de Dados PostgreSQL...")
    try:      
        #Conexão com o banco de dados do Mapa Produção
        conn = psycopg2.connect(
            host = '',
            database = '',
            user = '',
            port = '',
            password = ''
            )
        
        #Realiza as conexões 
        cur = conn.cursor()
        
        cur.execute("SELECT version();")
    
        rec = cur.fetchone()
        print("You are connected to - ", rec, "\n")
        
        #EXECUTA AS QUERRIES AQUI 
        
        #Querry para criar a tabela cnesEstabelecimentos se não existir
        #e truncar a tabela para inserção dos novos dados
        sql = '''CREATE TABLEE IF NOT EXISTS cnesEstabelecimentos(
        CO_UNIDADE TEXT,
        CO_CNES TEXT,
        NU_CNPJ_MANTENEDORA TEXT, 
        NO_RAZAO_SOCIAL TEXT, 
        NO_FANTASIA TEXT, 
        NO_LOGRADOURO TEXT, 
        NU_ENDERECO TEXT, 
        NO_COMPLEMENTO TEXT,
        NO_BAIRRO TEXT,
        CO_CEP TEXT,
        NU_TELEFONE TEXT, 
        NO_EMAIL TEXT,
        NU_CPF TEXT,
        NU_CNPJ TEXT,
        TP_UNIDADE TEXT,
        CO_MUNICIPIO_GESTOR TEXT,
        NU_LATITUDE TEXT,
        NU_LONGITUDE TEXT,
        CO_TIPO_ESTABELECIMENTO TEXT,
        CO_ATIVIDADE_PRINCIPAL TEXT);
        
        sql = 'TRUNCATE TABLE cnesEstabelecimentos;'''
        cur.execute(sql)
        conn.commit() 
        
        for i in tqdm(data.index):
            
            #insere os dados do Data Frame data
            sql = ''' INSERT INTO cnesEstabelecimentos 
            (CO_UNIDADE,CO_CNES,NU_CNPJ_MANTENEDORA,NO_RAZAO_SOCIAL, 
            NO_FANTASIA,NO_LOGRADOURO,NU_ENDERECO, NO_COMPLEMENTO,
            NO_BAIRRO,CO_CEP,NU_TELEFONE,NO_EMAIL,NU_CPF,
            NU_CNPJ,TP_UNIDADE,CO_MUNICIPIO_GESTOR,
            NU_LATITUDE,NU_LONGITUDE,CO_TIPO_ESTABELECIMENTO,
            CO_ATIVIDADE_PRINCIPAL)
            values ('%s','%s','%s','%s','%s','%s','%s','%s',
            '%s','%s','%s','%s','%s','%s','%s',
            '%s','%s','%s','%s','%s');''' % (data["CO_UNIDADE"][i],
            data["CO_CNES"][i],data["NU_CNPJ_MANTENEDORA"][i],data["NO_RAZAO_SOCIAL"][i],
            data["NO_FANTASIA"][i],data["NO_LOGRADOURO"][i],data["NU_ENDERECO"][i],
            data["NO_COMPLEMENTO"][i],data["NO_BAIRRO"][i],data["CO_CEP"][i],
            data["NU_TELEFONE"][i],data["NO_EMAIL"][i],data["NU_CPF"][i],
            data["NU_CNPJ"][i],data["TP_UNIDADE"][i],data["CO_MUNICIPIO_GESTOR"][i],
            data["NU_LATITUDE"][i],data["NU_LONGITUDE"][i],
            data["CO_TIPO_ESTABELECIMENTO"][i],data["CO_ATIVIDADE_PRINCIPAL"][i])
            
            cur.execute(sql)
            conn.commit()
        print("Table was succesfullt updated")
        
        #-------------------------------
        
        return conn
    
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    
    finally:
        if (conn):
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed")
            
Connect()
