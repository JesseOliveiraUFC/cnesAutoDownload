import pandas as pd
import wget
from tqdm import tqdm
from zipfile import ZipFile
import os
import psycopg2
from psycopg2 import Error

competencia = 202207 #É Necessário Digitar a competência que deseja buscar.
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
    zip.extract(f"rlEstabServClass{competencia}.csv", path=save_dir)

#Faz a leitura dos dados para um Data Frame (data) para realizar o tratamento
#dos dados.
def ReadAndSaveData():
    global data
    global telefone
    #Leitura dos dados
    data = pd.read_csv(save_dir+f"tbEstabelecimento{competencia}.csv", 
                       delimiter = ";", low_memory=False)
    atende_sus = pd.read_csv(save_dir+f"rlEstabServClass{competencia}.csv", 
                       delimiter = ";", low_memory=False)
    
    #Inserindo uma coluna de competência na tabala de dados
    competencia_list = pd.DataFrame([competencia for i in data.index], columns = ['COMPETENCIA'])
    data = pd.concat([competencia_list, data], axis = 1)
    
    
    #Eliminio todos os dados que não são do Ceará, ou seja, aqueles em que o 
    #CO_ESTADO_GESTOR são diferentes de 23. O Código do Ceará é 23.
    data = data.loc[(data['CO_ESTADO_GESTOR'] == 23)]
    telefone = data[['CO_UNIDADE','NU_TELEFONE']]
    
    #Convertendo o Data Frame para dados tipo STRING
    data = data.astype(str) 
    atende_sus = atende_sus.astype(str)
    
    
    
    #Separa os dados de telefone para serem analisados separadamente
    
    
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
                       'TP_LIC_SANI','NU_TELEFONE',
                       'CO_USUARIO','CO_CPFDIRETORCLN',
                       'REG_DIRETORCLN','ST_ADESAO_FILANTROP',
                       'CO_MOTIVO_DESAB',
                       'NO_URL',
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
    
    data = data.rename(columns = {"TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')":"DT_ATUALIZACAO"})
    
    atende_sus = atende_sus.drop(columns =['CO_SERVICO',
                        'CO_CLASSIFICACAO','TP_CARACTERISTICA',
                        'CO_CNPJCPF','CO_AMBULATORIAL',
                        'CO_HOSPITALAR','CO_END_COMPL',
                        'ST_ATIVO_SN','TO_CHAR(DT_ATUALIZACAO,\'DD/MM/YYYY\')',
                        'CO_USUARIO'
                        ])
  
    atende_sus_list = ['NÃO' if (atende_sus['CO_AMBULATORIAL_SUS'][i] == '2' and atende_sus['CO_HOSPITALAR_SUS'][i] == '2') else 'SIM' for i in range(len(atende_sus['CO_AMBULATORIAL_SUS']))]
    
    atende_sus['ATENDE_SUS'] = atende_sus_list
    atende_sus = atende_sus.drop(columns =['CO_AMBULATORIAL_SUS','CO_HOSPITALAR_SUS'])                                   
    
    
    data = pd.merge(data, atende_sus, how="left", on="CO_UNIDADE")
    data = data.drop_duplicates()
   
    
ReadAndSaveData()

#------------------------------------------------------------------------------
#_________________  Fazendo a Normalização dos telefones  _____________________

def PhoneTreatment():
    global telefone
    global data
    telefone = telefone.dropna()
    telefone = telefone.loc[(telefone['NU_TELEFONE'] != '( )')]
    telefone = telefone.reset_index(drop = True)
    
    listaInitial = telefone.values.tolist()
    lista = [item[1] for item in listaInitial]
    
    def NumericChar(numero):
        return ''.join([digit for digit in numero if digit.isnumeric()])
    
    def InitialZero(listaTelefonica):
        for i in range(len(listaTelefonica) - 1):
            if listaTelefonica[i][0] == '0':
                listaTelefonica[i] = listaTelefonica[i][1:]
        return(listaTelefonica)

    def RemoveDDD(listaTelefonica):
        for i in range(len(listaTelefonica) - 1):
            if listaTelefonica[i][0] == '8' and len(listaTelefonica) >= 10:
                listaTelefonica[i] = listaTelefonica[i][2:]
        return(listaTelefonica)
    
    def DigitFilter(listaTelefonica):
        result = [number if (len(number) == 8) or (len(number) == 9 and number[0] == '9') else '' for number in listaTelefonica]
        return(result)
    
    def SizeFilter(listaTelefonica,val1,val2,val3):
        result = [num if (len(num) == val1 or len(num) == val2 or len(num) == val3) else '' for num in listaTelefonica]
        return(result)
    
    def Treatment(listaTelefonica):
        print("Realizando o processo de Normalização dos dados de telefone")
        print("Elimindando dados não-numéricos...")
        listaTelefonica = [NumericChar(numero) for numero in listaTelefonica if len(NumericChar(numero)) >= 1]
        print("Eliminando o zero inicial")
        listaTelefonica = InitialZero(listaTelefonica)
        print("Eliminando o DDD")
        listaTelefonica = RemoveDDD(listaTelefonica)
        #print("Eliminando dados com tamanhos fora do escopo")
        #listaTelefonica = SizeFilter(listaTelefonica, 8, 9, 16)
        print("Elimina números com nove dígitos que não iniciam com 9")
        listaTelefonica = DigitFilter(listaTelefonica)
        print("Limpeza completa")
        return(listaTelefonica)
    
    lista_tratada = Treatment(lista)
    telefone['NU_TELEFONE'] = lista_tratada
    
    #Unindo os dados dos estabelecimentos com os dados de telefone
    data = pd.merge(data, telefone, how="left", on="CO_UNIDADE")    
    data = data.astype(str)
    data = data.reset_index(drop = True)
    
    #Salva os dados em um arquivo .csv local
    data.to_csv(save_dir+f"Estab-{competencia}.csv", index=False)
    
PhoneTreatment()
        
#------------------------------------------------------------------------------
#____________________  Fazendo a unificação dos dados  ________________________



#Realizar a conexão com o banco de dados
def Connect():
    global data
    print("Conectando ao Banco de Dados PostgreSQL...")
    try:      
        #Conexão com o banco de dados do Mapa Produção
        conn = psycopg2.connect(
            host = '54.232.0.124',
            database = 'dw',
            user = 'dwuser',
            port = '32466',
            password = '2QmpbKsQCsnpUP2T'
            )
        
        #Realiza as conexões 
        cur = conn.cursor()
        
        cur.execute("SELECT version();")
    
        rec = cur.fetchone()
        print("You are connected to - ", rec, "\n")
        
        #EXECUTA AS QUERRIES AQUI 
        
        #Querry para criar a tabela cnesEstabelecimentos se não existir
        #e truncar a tabela para inserção dos novos dados
        sql = '''DROP TABLE cnesEstabelecimentos;
        
        CREATE TABLE IF NOT EXISTS cnesEstabelecimentos(
        COMPETENCIA TEXT,
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
        NO_EMAIL TEXT,
        NU_CPF TEXT,
        NU_CNPJ TEXT,
        TP_UNIDADE TEXT,
        CO_MUNICIPIO_GESTOR TEXT,
        DT_ATUALIZACAO TEXT,
        NU_LATITUDE TEXT,
        NU_LONGITUDE TEXT,
        CO_TIPO_ESTABELECIMENTO TEXT,
        CO_ATIVIDADE_PRINCIPAL TEXT,
        ATENDE_SUS TEXT,
        NU_TELEFONE TEXT);
        '''
        
        cur.execute(sql)
        conn.commit() 
        
        print("Tabela Criada com Sucesso")
        
        for i in tqdm(data.index):
            
            print(i)
            
            #insere os dados do Data Frame data
            sql = ''' INSERT INTO cnesEstabelecimentos 
            (COMPETENCIA,CO_UNIDADE,CO_CNES,NU_CNPJ_MANTENEDORA,NO_RAZAO_SOCIAL, 
            NO_FANTASIA,NO_LOGRADOURO,NU_ENDERECO, NO_COMPLEMENTO,
            NO_BAIRRO,CO_CEP,NO_EMAIL,NU_CPF,
            NU_CNPJ,TP_UNIDADE,CO_MUNICIPIO_GESTOR,DT_ATUALIZACAO,
            NU_LATITUDE,NU_LONGITUDE,CO_TIPO_ESTABELECIMENTO,
            CO_ATIVIDADE_PRINCIPAL,ATENDE_SUS,NU_TELEFONE)
            VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s',
            '%s','%s','%s','%s','%s','%s','%s',
            '%s','%s','%s','%s','%s','%s','%s');''' % (data["COMPETENCIA"][i],data["CO_UNIDADE"][i],
            data["CO_CNES"][i],data["NU_CNPJ_MANTENEDORA"][i],data["NO_RAZAO_SOCIAL"][i],
            data["NO_FANTASIA"][i],data["NO_LOGRADOURO"][i],data["NU_ENDERECO"][i],
            data["NO_COMPLEMENTO"][i],data["NO_BAIRRO"][i],data["CO_CEP"][i],
            data["NO_EMAIL"][i],data["NU_CPF"][i],data["NU_CNPJ"][i],
            data["TP_UNIDADE"][i],data["CO_MUNICIPIO_GESTOR"][i],
            data["DT_ATUALIZACAO"][i],["NU_LATITUDE"][i],data["NU_LONGITUDE"][i],
            data["CO_TIPO_ESTABELECIMENTO"][i],data["CO_ATIVIDADE_PRINCIPAL"][i],
            data["ATENDE_SUS"][i],data["NU_TELEFONE"][i])
    
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
