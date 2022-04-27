from concurrent.futures import ThreadPoolExecutor
import queue, requests
from bs4 import BeautifulSoup as BS
import re
from threading import * 
# pip install pdfminer
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams
from pdfminer.converter import TextConverter
from pdfminer.pdfpage import PDFPage
import io
import requests


#-----------Variables globales
visitados = []
procesados = []
noVisitadar = []
semaforoCola = Semaphore()  
palabrasClave = ['covid', 'pandemia', 'coronavirus']
COOLDOWN = 0.15
LIMIT = 2

'''
Funcion que transforma un pdf en texto para validar si contiene alguna palabra clave en sus primeras 5 paginas
'''
def comprobarPDF(pdf):
    try:
        f = io.BytesIO()
        f.write(pdf)
        txt = io.StringIO()
        rm = PDFResourceManager()
        conv = TextConverter(rm, txt, laparams=LAParams())
        inter = PDFPageInterpreter(rm, conv)
        # extraigo las primeras 5 paginas
        for page in PDFPage.get_pages(f, pagenos=(0, 1, 2, 3, 4)):
            inter.process_page(page)
        text = txt.getvalue()
        txt.close()
        #Expresion para verificar si existen las palabras clave en el texto
        reg = '|'.join(palabrasClave)
        resultado = re.findall(reg, text)
        #Se retorna la cantidad de coincidencias
        return len(resultado)
    except:
        return -1

'''
Se verifica si un url a comrpbar es de tipo pdf y si continene alguna de las 
palabras clave
'''
def comprobarUrl(urlComprobar,url):
    global noVisitadar, procesados
    #Se verifica que el url no haya sido visitados/comprobado anteriormente
    if(urlComprobar not in visitados):
        urlVerificar = urlComprobar
        semilla = url.split('/')
        dividido = urlComprobar.split('/')
        #No posee dominio
        if(dividido[0] == ''):
            urlVerificar = "/".join(semilla[:3])+urlVerificar

        #si es mas corto que el url anterior, no valida porque se dirige a donde no nos interesa
        if(len(urlVerificar.split('/'))<len(semilla)):
            return False
        
        req = requests.get(urlVerificar, headers={'User-Agent': 'Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)'}, timeout=3)
        contentType = req.headers.get('content-type')

        #Si un pdf que no ha sido agregado y no esta en la lista de no revisitar, se agrega a los procesados
        if 'application/pdf' in contentType and urlVerificar not in procesados and urlVerificar not in noVisitadar:
            #Validacion de contenido del pdf
            if(comprobarPDF(req.content)>0):
                procesados.append(urlVerificar)
            return False

'''
Funcion que extrae todos los hipervinculos de un url por parametro, bajo cierto criterio
Luego, comprueba si cada uno es un archivo pdf
'''

def procesador(url):
    req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)'}, timeout=3)
    soup = BS(req.text, "html.parser")
    #Se eliminan secciones no necesarias del html
    if  soup.footer:
        soup.footer.decompose()
    if soup.header:
        soup.header.decompose()
    if soup.navbar:
        soup.navbar.decompose()
    if soup.head: 
         soup.head.decompose()
    #Se extraen los hipervinculos que sean relacionados a las semillas
    exp = url+'|^/'
    for link in soup.find_all(attrs={'href': re.compile(exp)}):
        urlComprobar = link.get('href')
        #Se verifica el url extraido
        comprobarUrl(urlComprobar, url)
    

 
def calendarizador_corto(root, rv, nvi):
    global no_visitadar, procesados
    no_visitadar = nvi
    #los que se revisitan van directo a la lista de procesados
    procesados = rv
    for i in root:
        procesador(i)
        
    return procesados
