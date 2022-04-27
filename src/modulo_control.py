import caledarizador_largo as cl
import calendarizador_corto as cc
import descargador as d


linksSemilla, listaRevisitar, listaNoRevisitar = cl.calendarizadorLargoPlazo('./src/archivo_de_configuracion.json')
links = cc.calendarizador_corto(linksSemilla, listaRevisitar, listaNoRevisitar)
d.download_docs(links)