from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import queue, time, requests
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
cola = queue.Queue()
visitados = []
procesados = []
noVisitadar = []
contadores = []
semaforoCola = Semaphore()  
expresion = ''
palabrasClave = ['covid', 'pandemia', 'coronavirus']
COOLDOWN = 0.15
LIMIT = 20
'''
Funcion que crea la cola inicial con mis link semilla, ademas genera una expresion con las semillas para
posteriormente serutilizada en el filtrado del contenido de las paginas
'''
def crearColaSemillas(urls , spacing):
    global expresion
    #Por cada url lo agrego a la cola y adiciono a la expresion
    for url in urls:
        time.sleep(spacing)
        cola.put((url, 0))

    return "Termino cargar los URLS"

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
        return 0


'''

'''
def comprobarUrl(urlComprobar,url):

    global noVisitadar, procesados
    #Se verifica que el url no haya sido visitados/comprobado anteriormente
    if(urlComprobar not in visitados):
   
        urlVerificar = urlComprobar
        semilla = url[0].split('/')
        dividido = urlComprobar.split('/')
        #No posee dominio
        if(dividido[0] == ''):
            urlVerificar = "/".join(semilla[:3])+urlVerificar

        #si es mas corto que el url anterior, no valida porque se dirige a donde no nos interesa
        if(len(urlVerificar.split('/'))<len(semilla)):
            return False
        #Se agreaga a la lista de url ya comprobados
        visitados.append(urlVerificar) 

        req = requests.get(urlVerificar, headers={'User-Agent': 'Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)'}, timeout=3)
        contentType = req.headers.get('content-type')
        
        #Si un pdf que no ha sido agregado y no esta en la lista de no revisitar, se agrega a los procesados
        if 'application/pdf' in contentType and urlVerificar not in procesados and urlVerificar not in noVisitadar:
            #Validacion de contenido del pdf
            if(comprobarPDF(req.content)>0):
                procesados.append(urlVerificar)
            return False
        #Si la comprobacion retorna algo, significa que no era pdf y se agrega a la cola
        if(url[1]+1<LIMIT):
            semaforoCola.acquire()
            cola.put((urlVerificar, url[1]+1))
            semaforoCola.release()


def procesador(url):
    req = requests.get(url[0], headers={'User-Agent': 'Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)'}, timeout=3)
    soup = BS(req.text, "html.parser")
    #Cola para los hipervinculos encontrados en el html
    urls = queue.Queue()
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
    exp = url[0]+'|^/'
    for link in soup.find_all(attrs={'href': re.compile(exp)}):
        urls.put(link.get('href'))

    urlComprobar = ""

    #with ThreadPoolExecutor(max_workers=10) as exec:
    while not urls.empty():
        semaforoCola.acquire()  
        urlComprobar = urls.get()
        semaforoCola.release()
        comprobarUrl(urlComprobar, url)
    return urlComprobar

 
def calendarizador_corto(root, rv, nvi):
    global no_visitadar, procesados
    no_visitadar = nvi
    #los que se revisitan van directo a la lista de procesados
    procesados = rv
    
    with ThreadPoolExecutor(max_workers=25) as executor:
        #SEMILLAS
        future_to_url = {
            executor.submit(crearColaSemillas, root, COOLDOWN): 'FEEDER DONE'}

        while future_to_url:
            done, not_done = concurrent.futures.wait(
                future_to_url, timeout=COOLDOWN,
                return_when=concurrent.futures.ALL_COMPLETED)
            
            while not cola.empty():
                semaforoCola.acquire()
                url = cola.get()
                print("------------ LINK", url)
                semaforoCola.release()
                future_to_url[executor.submit(procesador, url)] = url

            for future in done:
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    if url == 'FEEDER DONE':
                        print(data)

                # eliminar el futuro ahora completado
                del future_to_url[future]

semillas = [
    "https://www.ict.go.cr/en/documents/material-de-apoyo-coronavirus/protocolos-ict-sector-privado.html",
    "https://repositorio.conare.ac.cr/bitstream/handle/20.500.12337/8009/Leon_G_Efectos_pandemia_COVID_19_Mipymes_Costa%20Rica_2020.pdf?sequence=1&isAllowed=y",
    "https://www.ministeriodesalud.go.cr/index.php/biblioteca-de-archivos-left/documentos-ministerio-de-salud/vigilancia-de-la-salud/normas-protocolos-guias-y-lineamientos/situacion-nacional-covid-19/estrategias-guias-y-recomendaciones-covid-19"
]
revisados = []
noVisitar = []

calendarizador_corto(semillas, revisados, noVisitar)