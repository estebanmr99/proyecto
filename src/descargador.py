# Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)

# download document files and save to local files concurrently
from os import makedirs
from os.path import basename
from os.path import join, exists
from urllib.request import urlopen, Request
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import time
import queue
import requests
import random
import preprocesamiento as pp

q = queue.Queue()
PATH = '../documentos/docsOriginales/'
LINKS = []
COOLDOWN = 0.15

# python concurrency API docs
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

def feed_the_workers(urls , spacing):
    """ Simulate outside actors sending in work to do, request each url twice """
    for url in urls:
        time.sleep(spacing)
        q.put(url)
    return "DONE FEEDING"

# download a url and return the raw data, or None on error
def download_url(url):
    try:
        # open a connection to the server
        req = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (compatible; ProjectCrawlerBot/1.0;)'}, timeout=3)
        content_type = req.headers.get('content-type')

        if 'application/pdf' in content_type:
            ext = '.pdf'
        elif 'text/html' in content_type:
            ext = '.html'
        else:
            ext = ''
            print('Unknown type: {}'.format(content_type))

        # read the contents of the html doc
        return req.content, ext

    except:
        # bad url, socket timeout, http forbidden, etc.
        return None, None
 
# save data to a local file
def save_file(fileName, data, path):
    # construct a local path for saving the file
    outpath = join(path, fileName)
    # save to file
    with open(outpath, 'wb') as file:
        file.write(data)
    return outpath
 
def checkFileName(path, fileName):
    outpath = join(path, fileName)
    newFileName = fileName
    randomInt = random.randint(0, 999)
    while(exists(outpath)):
        newFileName = fileName.replace(".pdf", str(randomInt) + ".pdf").replace(".html", str(randomInt) + ".html")
        outpath = join(path, newFileName)
        randomInt = random.randint(0, 999)

    return newFileName

# download and save a url as a local file
def download_and_save(url, path):
    # download the url
    data, ext = download_url(url)
    # check for no data
    if data is None:
        print(f'>Error downloading {url}')
        return
    # get the name of the file from the url
    fileName = basename(url).replace(".pdf", "").replace(".html", "") + ext

    fileName = checkFileName(path, fileName)

    # save the data to a local file
    outpath = save_file(fileName, data, path)
    # report progress
    # print(f'>Saved {url} to {outpath}')

    # ------------------------------------------------------------------------------------- poner semaforo
    if(exists(outpath)):
        newLink = {
            "link": url,
            "path-texto-original": join(path, fileName),
        }
        LINKS.append(newLink);
 
# download a list of URLs to local files
def download_docs(urls):
    path = PATH
    # create the thread pool
    with ThreadPoolExecutor(max_workers=25) as executor:
        future_to_url = {
            executor.submit(feed_the_workers, urls, COOLDOWN): 'FEEDER DONE'}

        while future_to_url:
            # check for status of the futures which are currently working
            done, not_done = concurrent.futures.wait(
                future_to_url, timeout=COOLDOWN,
                return_when=concurrent.futures.FIRST_COMPLETED)

            # if there is incoming work, start a new future
            while not q.empty():

                # fetch a url from the queue
                url = q.get()

                # Start the load operation and mark the future with its URL
                future_to_url[executor.submit(download_and_save, url, path)] = url

            # process any completed futures
            for future in done:
                url = future_to_url[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (url, exc))
                else:
                    if url == 'FEEDER DONE':
                        print(data)
                    # else:
                    #     print('%r page is %d bytes' % (url, len(data)))

                # remove the now completed future
                del future_to_url[future]
    
    n_threads = len(LINKS)
    with ThreadPoolExecutor(n_threads) as executor:
        # download each url and save as a local file
        _ = [executor.submit(pp.preprocessingFile, link["path-texto-original"]) for link in LINKS]

 
# download all docs
download_docs(URLS)