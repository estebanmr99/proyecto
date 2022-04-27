from os.path import basename
from os.path import join, exists
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
import queue
import requests
import preprocesamiento as pp
import threading
import os
from datetime import datetime
import json
from dateutil.parser import parse as parsedate

# variables generales
semaphoreLinks = threading.Semaphore()
semaphoreSavingJSON = threading.Semaphore()
q = queue.Queue()
PATH = '../documentos/docsOriginales/'
LINKS = []
COOLDOWN = 0.15

# lista de URLS a descargar
URLS = [
        'https://www.clickdimensions.com/links/TestPDFfile.pdf',
        'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',
        'https://repositorio.conare.ac.cr/bitstream/handle/20.500.12337/8286/Gomez_S_Personas_afectadas_pandemia_demanda_bono_Proteger_IEN_2022.pdf?sequence=1&isAllowed=y',
        'https://www.ict.go.cr/en/documents/material-de-apoyo-coronavirus/pruebas-covid-para-usa/1886-faq-requirements-for-travel-to-eeuu/file.html',
        'https://www.ministeriodesalud.go.cr/index.php/biblioteca-de-archivos-left/documentos-ministerio-de-salud/vigilancia-de-la-salud/normas-protocolos-guias-y-lineamientos/situacion-nacional-covid-19/lineamientos-especificos-covid-19/lineamientos-de-servicios-de-salud/5038-version-3-08-de-abril-2020-lineamientos-generales-para-farmacias-de-comunidad-privadas-frente-a-la-pandemia-por-covid-19/file',
        'https://www.ministeriodesalud.go.cr/index.php/biblioteca/material-educativo/material-de-comunicacion/salud-mental/4103-consejos-para-disminuir-el-estres-de-ninos-y-ninas-durante-la-pandemia/file',
        'https://www.ministeriodesalud.go.cr/index.php/biblioteca-de-archivos-left/documentos-ministerio-de-salud/vigilancia-de-la-salud/normas-protocolos-guias-y-lineamientos/situacion-nacional-covid-19/lineamientos-especificos-covid-19/protocolos-1/3655-version-3-19-de-abril-2021-protocolos-para-la-operacion-paulatina-del-aeropuerto-internacional-daniel-oduber-quiros-durante-la-pandemia-por-covid-19-posterior-a-la-apertura-de-fronteras-costa-rica/file',
        'https://www.ministeriodesalud.go.cr/index.php/biblioteca-de-archivos-left/documentos-ministerio-de-salud/vigilancia-de-la-salud/normas-protocolos-guias-y-lineamientos/situacion-nacional-covid-19/lineamientos-especificos-covid-19/protocolos-1/2756-version-1-05-de-octubre-2020-protocolo-general-para-el-manejo-del-paro-cardiorespitatorio-pcr-en-el-entorno-extrahospitalario-y-en-el-marco-de-la-pandemia-por-covid-19/file',
        'https://docs.python.org/3/library/concurrency.html']

# agrega una lista de URLS a una cola de prioridad para ejecutar los hilos
def addURLsToThreadQueue(urls , spacing):
    for url in urls:
        time.sleep(spacing)
        q.put(url)
    return "DONE FEEDING"

# descarga un URL y devuelve los datos sin procesar, o None en caso de error
def download_url(url):
    try:
        # abrir una conexión al servidor
        req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)'}, timeout=3)
        content_type = req.headers.get('content-type')
        last_modified = req.headers.get('last-modified')

        if 'application/pdf' in content_type:
            ext = '.pdf'
        elif 'text/html' in content_type:
            ext = '.html'
        else:
            ext = ''
            print('Unknown type: {}'.format(content_type))

        # leer el contenido del documento html
        return req.content, ext, last_modified

    except:
        # URL incorrecta, tiempo de espera de socket, http prohibido, etc.
        return None, None, None
 
# guardar datos en un archivo local
def save_file(fileName, data, path):
    # construye una ruta local para guardar el archivo
    outpath = join(path, fileName)
    # Guardar en archivo
    with open(outpath, 'wb') as file:
        file.write(data)
    return outpath
 
# verifica que el nombre del pdf se pueda guardar sino genera un nuevo
def checkFileName(path, fileName, last_modified):
    outpath = join(path, fileName)
    newFileName = fileName
    # si el nombre existe se crea uno con la fecha de modificacion
    if(exists(outpath)):
        fileDatePlusTime = parsedate(last_modified).strftime("%Y-%m-%d-%H:%M:%S")
        newFileName = fileName.replace(".pdf", str(fileDatePlusTime) + ".pdf").replace(".html", str(fileDatePlusTime) + ".html")

    return newFileName

# descargar y guardar una url como un archivo local
def download_and_save(url, path):
    # descargar el URL
    data, ext, last_modified = download_url(url)
    # revisar si descarga funciono
    if data is None:
        print(f'>Error downloading {url}')
        return
    # obtener el nombre del archivo de la url
    fileName = basename(url).replace(".pdf", "").replace(".html", "") + ext

    fileName = checkFileName(path, fileName, last_modified)

    # guardar los datos en un archivo local
    outpath = save_file(fileName, data, path)

    # Si el archivo se guardo bien se 
    if(exists(outpath)):
        semaphoreLinks.acquire()
        newLink = {
            "link": url,
            "path-texto-original": join(path, fileName),
        }
        LINKS.append(newLink);
        semaphoreLinks.release()

# se agrega el archivo procesado al almacen
def procesingFile(linkDic):
    # se obtienen todos los metadatos necesarios del archivo
    fileName = os.path.basename(linkDic["path-texto-original"])
    fileCreationDate = datetime.fromtimestamp(os.path.getctime(linkDic["path-texto-original"])).strftime("%d/%m/%Y")
    fileModificationDate = datetime.fromtimestamp(os.path.getmtime(linkDic["path-texto-original"])).strftime("%d/%m/%Y")
    fileAccessDate = datetime.today().strftime("%d/%m/%Y")
    processedFilePath = "../documentos/almacen/textoProcesado/" + fileName.replace(".pdf", ".txt")

    # se agregan los metadato al diccionario
    linkDic["fecha-ingreso"] = fileAccessDate
    linkDic["fecha-reingreso"] = fileAccessDate
    linkDic["titulo"] = fileName
    linkDic["fecha-creacion-documento"] = fileCreationDate
    linkDic["fecha-actualizacion-documento"] = fileModificationDate
    linkDic["path-texto-enriquecido"] = processedFilePath

    almacenFilePath = "../documentos/almacen/almacen.json"

    # se pide el semaforo para guardar el nuevo json en el almacen
    semaphoreSavingJSON.acquire()
    with open(almacenFilePath, "r") as jsonFile:
        data = json.load(jsonFile)

    isLinkPresent = False
    # buscar si la llave existia previamente en el almacen
    for i in range (0, len(data["links"])):
        if (data["links"][i]["link"] == linkDic["link"]):
            data["links"][i]["fecha-reingreso"] = fileAccessDate
            data["links"][i]["fecha-actualizacion-documento"] = fileModificationDate
            data["links"][i]["path-texto-enriquecido"] = processedFilePath
            isLinkPresent = True
            break
    
    # si la llave no existia se agrega todo el diccionario del url
    if (not isLinkPresent):
        data["links"].append(linkDic)

    # guarda el json modificado en el almacen
    with open(almacenFilePath, "w") as jsonFile:
        json.dump(data, jsonFile)

    #devuelve el semaforo
    semaphoreSavingJSON.release()
 
# descargar una lista de URL a archivos locales
def download_docs(urls):
    path = PATH
    
    # crear un thread pool
    with ThreadPoolExecutor(max_workers=25) as executor:
        future_to_url = {
            executor.submit(addURLsToThreadQueue, urls, COOLDOWN): 'FEEDER DONE'}

        while future_to_url:
            # comprobar el estado de los futuros que están funcionando actualmente
            done, not_done = concurrent.futures.wait(
                future_to_url, timeout=COOLDOWN,
                return_when=concurrent.futures.FIRST_COMPLETED)

            # si hay trabajo entrante, comienza un nuevo futuro
            while not q.empty():

                # obtener una URL de la cola
                url = q.get()

                # inicia la operación de descarga y marca el futuro con el URL
                future_to_url[executor.submit(download_and_save, url, path)] = url

            # procesar cualquier futuro completado
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
    
    n_threads = len(LINKS)
    with ThreadPoolExecutor(n_threads) as executor:
        # procesa cada archivo para guardarse en el almacen (JSON)
        _ = [executor.submit(procesingFile, link) for link in LINKS]

    with ThreadPoolExecutor(n_threads) as executor:
        # hace el preprocesamiento para cada archivo descargado
        _ = [executor.submit(pp.preprocessingFile, link["path-texto-original"]) for link in LINKS]
    
# descargar todos los archivos
# download_docs(URLS)