from distutils.command.config import config
import json
from datetime import datetime, timedelta
from re import sub
import tldextract 

def calendarizadorLargoPlazo(rutaArchivoConfig):

    listaConfig, linksSemilla = leerArchivoConfiguracion(rutaArchivoConfig)
    listaAlmacen, linksOriginales = leerArchivoAlmacen()
    listaRevisitar, listaNoRevisitar = actualizadorFechas(listaConfig, listaAlmacen, linksOriginales)

    #print("\n Lista revisitar: ", listaRevisitar, "\n")
    #print("Lista no revisitar: ", listaNoRevisitar, "\n")
    #print("Lista de links semillas: ", linksSemilla, "\n")
    return linksSemilla, listaRevisitar, listaNoRevisitar
    ###################################################

def leerArchivoConfiguracion(rutaArchivoConfig):
    # Leer archivo de configuracion
    configFile = open(rutaArchivoConfig) # Se abre el archivo .json
    dataConfig = json.load(configFile) # Se tiene el objeto .json como un diccionario

    listaConfig = [] # Se crea lista con los links semillas y el tiempo de revisitacion\
    linksSemilla = [] # Se crea la lista con los links originales
    
    # Se itera sobre la lista .json en la seccion de los link semillas 
    for linea in dataConfig['configuracion']['linkSemillas']:
        
        # Se obtiene el dominio del sitio
        # Se agarra el subdomain
        subdominio = tldextract.extract(linea['link']).subdomain
        # Si es www no se toma
        link = ""
        if (subdominio == "www"):
            pass
        # Si no es www se toma
        else:
        # Se agrega a la lista de dominios
            link += subdominio + "."
        link += tldextract.extract(linea['link']).domain


        # Se agregan a la lista
        listaConfig.append(link)
        listaConfig.append(linea['tiempo'])
        linksSemilla.append(linea['link'])

    # Se cierra el archivo
    configFile.close()
    
    # Se devuelve la lista
    return listaConfig, linksSemilla
    ###################################################

def leerArchivoAlmacen():
    # Leer almacen de datos 
    # Se abre el archivo .json
    almacenFile = open('../documentos/almacen/almacen.json')
    almacenData = json.load(almacenFile) # Se tiene el objeto .json como un diccionario

    # Se crea una lista con los links del almacen y la ultima fecha que se visito
    listaAlmacen = []
    # Se crea la lista con los links semillas
    linksOriginales = []

    # Se itera sobre la lista .json
    for linea in almacenData['links']:

        # Se obtiene el dominio del sitio
        # Se agarra el subdomain
        subdominio = tldextract.extract(linea['link']).subdomain
        # Si es www no se toma
        link = ""
        if (subdominio == "www"):
            pass
        # Si no es www se toma
        else:
        # Se agrega a la lista de dominios
            link += subdominio + "."
        link += tldextract.extract(linea['link']).domain


        # Se agregan a la lista
        listaAlmacen.append(link)
        listaAlmacen.append(linea['fecha-reingreso'])
        linksOriginales.append(linea['link'])
        linksOriginales.append("-")

    # Se cierra el archivo
    almacenFile.close()
    
    # Se devuelve la lista
    return listaAlmacen, linksOriginales
    ###################################################

def actualizadorFechas(listaConfig, listaAlmacen, linksOriginales):

    #recorro lista almacen
    #comparo los dominios con los de listaConfig
    #si son iguales, verifico politicas de revistacion
        #Si hace falta revisitar, agrego a lista de visitar
        #si no hace falta visitar, agrego a lista de no visitar
    #si no existe, lo agrego a la lista de visitar

    # Lista de links a revisitar
    listaRevisitar = []
    # Lista de links de no visitar
    listaNoRevisitar = []

    # Itero sobre la lista de links del almacen
    for i in range(0, len(listaAlmacen), 2):

        # Obtengo el dominio de los links
        dominio = listaAlmacen[i]
        linkOriginal = linksOriginales[i]

        # Bandera para saber si el dominio existe en los links semillas
        bandera = 0
        # Itero sobre la lista de las semillas
        for j in range(0, len(listaConfig), 2):

            # Obtengo el dominio del link semilla
            dominioSemilla = listaConfig[j]

            if(dominio == dominioSemilla):

                # Se cambia la bandera a 1, siendo asi que si hay un link semilla para el link
                bandera = 1

                # Se obtiene la fecha y hora actual
                x = datetime.now()
                # Se parsea para que quede en formado dd/mm/yyyy
                fechaActual = x.strftime("%x")

                # Obtengo la cantidad de dias de la politica de verificacion en la lista config
                dias = int(listaConfig[j+1].split(" ")[0])
                # Obtengo la fecha del link del almacen
                fechaNueva = listaAlmacen[i+1]
                # Le cambio el formato a la fecha del almacen
                dt = datetime.strptime(fechaNueva, '%d/%m/%y')
                # Le sumo los dias a la fecha de la lista almacen
                dt += timedelta(days=dias)
                # Vuelvo al formato dd/mm/yyyy
                fechaNueva = dt.strftime('%d-%m-%Y')

                # Si esa fecha es mayor a la actual se agrega a la no visitar
                if (fechaActual > fechaNueva):
                    # Si no hace falta revisitar
                    listaNoRevisitar.append(linkOriginal)

                # Si esa fecha es menor o igual a la actual se agrega a revisitar
                if (fechaActual <= fechaNueva):
                    # Si hace falta revisitar
                    listaRevisitar.append(linkOriginal)

        # Si no estaba el dominio en los links semillas
        if (bandera == 0):
            # Se agrega a la lista de revisitar
            listaRevisitar.append(linkOriginal)

    # Se retornan las dos listas de revisitacion
    return listaRevisitar, listaNoRevisitar

