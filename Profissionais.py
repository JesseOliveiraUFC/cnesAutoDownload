import pandas as pd    
import requests     #Biblioteca de fazer requisição ao banco de dados GET, POST, PATCH, DELETE      #Biblioteca de fazer extração de arquivos .zip
from tqdm import tqdm     #Biblioteca de conexão com o MongoDB
import os           #BIblioteca para gerenciar o Sistema Operacional. Copiar, excluir, alterar pastas, etc. 
import wget         #Biblioteca para fazer download de arquivos de um site
import psycopg2
from psycopg2 import Error

#é necessário digitar a competência nas linhas 53 e 58. 
fileDirectory  = ""

class Cnes:
    def __init__(self, competencias, dir_path_save = 'data'):
        self.competencia = competencias
        self.dir_path_save = dir_path_save
    
    #Cria a pasta para armazenar os dados
    def _verifica_diretorio(self):
        try:
            os.mkdir(fileDirectory+self.dir_path_save)
            print(f'Diretório "{self.dir_path_save}" criado')
        except OSError as error:
            print(error)
    
    #Realiza o donwload dos dados caso ainda não tenham sido baixados
    def _baixar_dados_cnes(self):
        for compet in tqdm(self.competencia):
            if os.listdir(fileDirectory+f'{self.dir_path_save}/').count(f'{compet}.zip') == 0:
                url_data = 'https://cnes.datasus.gov.br/services/profissionais-url-download?estado=23&gestao=todos&comp='+str(compet)
                s = requests.Session()
                s.headers.update({'referer':
                                  'https://cnes.datasus.gov.br/pages/profissionais/consulta.jsp'})
                r = s.get(url_data)
                link_de_down = f"http://cnesdownload.datasus.gov.br/download/ProfissionaisServlet?path={r.json()['url']}"
                save_path = fileDirectory+f'{self.dir_path_save}/{compet}.zip'
                print(link_de_down)
                wget.download(link_de_down, save_path)    #ftp://ftp.datasus.gov.br/cnes/BASE_DE_DADOS_CNES_202206.ZIP
                print(compet,end=' ')
            else:
                print(compet,'(salvo)',end=' ')
    
    #Pega os dados da competência a ser analisada.
    def _get_dados_competencia(self, competencia):
        if os.listdir(fileDirectory+self.dir_path_save).count(str(competencia)+'.zip') == 0:
            print('Competência não existe')
            return None
        else:
            file_name   = fileDirectory+f'{self.dir_path_save}/{competencia}.zip'
            df          = pd.read_csv(file_name, compression='zip', header=0, sep=';', quotechar='"')
            df.to_csv(fileDirectory+f'{self.dir_path_save}/{competencia}'+'.csv',index = False)
            return df
        
#Linhas de código para baixar as competências 
#PS. Ele não vai baixar as competencias que já foram baixadas
#DEVE INSERIR A NOVA COMPETÊNCIA A SER BAIXADA AO FINAL DA LISTA cnes abaixo      
cnes = Cnes(['202201','202202','202203','202204','202205','202206']) 
cnes._baixar_dados_cnes()

#Uma vez que as competências foram baixadas, essa linha serve para extrair 
#os dados que foram baixados para serem enviados para o banco de dados
df = cnes._get_dados_competencia(202206)  #Escolha a competência


#Realiza a conexão com o Banco de Dados
def Connect():
    print("Conectando ao Banco de Dados PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host = '',
            database = '',
            user = '',
            port = '',
            password = ''
            )
     
        cur = conn.cursor()
    
        cur.execute("SELECT version();")
    
        rec = cur.fetchone()
        print("You are connected to - ", rec, "\n")
        
        #EXECUTA AS QUERRIES AQUI ------
        
        sql = '''       
            CREATE TABLE IF NOT EXISTS cnesProfissionais 
            (
            COMPETENCIA TEXT,
            NOME TEXT, CNS TEXT, 
            SEXO TEXT, IBGE TEXT, 
            UF TEXT, MUNICIPIO TEXT,
            CBO TEXT,
            DESCRICAO_CBO TEXT,
            CNES TEXT, CNPJ TEXT,
            ESTABELECIMENTO TEXT
            ); 
            
            TRUNCATE TABLE cnesProfissionais
            '''
        cur.execute(sql)
        conn.commit()
        
        print("Table created successfully in PostgreSQL \n")
        print("Printing the Data en PostgreSQL table")
        
        for i in tqdm(df.index):
            
            sql = ''' INSERT INTO cnesProfissionais 
            (COMPETENCIA, NOME, CNS, SEXO, IBGE, UF, MUNICIPIO,
            CBO, DESCRICAO_CBO, CNES, CNPJ, ESTABELECIMENTO)
            values ('%s','%s','%s','%s','%s','%s','%s','%s',
            '%s','%s','%s','%s');''' % (df["COMPETENCIA"][i],
            df["NOME"][i],df["CNS"][i],df["SEXO"][i],df["IBGE"][i],
            df["UF"][i],df["MUNICIPIO"][i],df["CBO"][i],
            df["DESCRICAO CBO"][i],df["CNES"][i],
            df["CNPJ"][i],df["ESTABELECIMENTO"][i])
            
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
            
def Connect2():
    print("Conectando ao Banco de Dados PostgreSQL...")
    
    conn = psycopg2.connect(
        host = '54.232.0.124',
        database = 'dw',
        user = 'dwuser',
        port = '32466',
        password = '2QmpbKsQCsnpUP2T'
        )
    return conn

Connect()            
