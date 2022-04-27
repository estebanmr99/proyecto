import caledarizador_largo as cl
import calendarizador_corto as cc
import descargador as d

print('Comienza calendarizador a largo plazo')
linksSemilla, listaRevisitar, listaNoRevisitar = cl.calendarizadorLargoPlazo('./archivo_de_configuracion.json')
print('Comienza calendarizador a corto plazo')
links = cc.calendarizador_corto(linksSemilla, listaRevisitar, listaNoRevisitar)
print('Comienza descargador')
d.download_docs(links)