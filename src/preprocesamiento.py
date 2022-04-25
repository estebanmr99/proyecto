import os
from nltk.corpus import stopwords # pip install nltk -> nltk.download()
from nltk.tokenize import word_tokenize
import spacy # pip install -U spacy
import string
import subprocess
from bs4 import BeautifulSoup# pip install beautifulsoup4

# preprocesa el arhivo
def preprocessingFile(filePath):
    #carga el paquete para hacer la lematizacion
    nlp = spacy.load("es_core_news_sm") # python -m spacy download en_core_web_sm
    fileName = os.path.basename(filePath)
    pathProcessedFiles = '../documentos/almacen/textoProcesado/'

    #verifica si el archivo a procesar es un pdf o html
    if (fileName.endswith('.pdf')):
        #pasa todo el pdf a txt
        command = './pdf2txtScript.sh ' + filePath
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
    elif (fileName.endswith('.html')):
        #pasa todo el archivo html a txt
        fileText = open(filePath, "r")
        all_text = BeautifulSoup(fileText, features="html.parser").text
        with open(pathProcessedFiles + fileName + '.txt','w', encoding = 'utf-8') as fileWithoutHTML:
            # guarda el texto pasado a texto
            fileWithoutHTML.write(all_text)

    # abre el archivo a pre procesar
    file = open(pathProcessedFiles + fileName + ".txt", "r", encoding="utf8")
    linea = file.read()
    # se pasa todo el texto a minuscula
    linea = linea.lower()
    #se tokenizan las palabras
    word_tokens = word_tokenize(linea)

    stop_words = set(stopwords.words('spanish'))
    
    #  Limpia de de signos de puntuacion, y de stopwords
    clean_words = []
    for word in word_tokens:
        #Se quitan signos de puntuacion
        if word in string.punctuation or word.isdigit():
            continue
            #se quita las stop words
        elif word not in stop_words:
            clean_words.append(word)

    # Lematizacion de las palabras
    lemmatized_words = []
    for i in range(len(clean_words)):
        sentence = nlp(clean_words[i])
        for word in sentence:
            lemmas=word.lemma_
            lemmatized_words.append(lemmas)

    # guarda las palabras lematizadas
    appendFile = open(pathProcessedFiles + fileName.replace(".pdf","").replace(".html","") +'.txt','w', encoding="utf8")

    for word in lemmatized_words:
        appendFile.write(" "+ word)

    appendFile.close()

# preprocessingFile('../documentos/docsOriginales/arannador.pdf');