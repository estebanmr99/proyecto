import os
from nltk.corpus import stopwords # pip install nltk -> nltk.download()
from nltk.tokenize import word_tokenize
import spacy # pip install -U spacy
import string
import subprocess
from bs4 import BeautifulSoup# pip install beautifulsoup4

def preprocessingFile(filePath):
    nlp = spacy.load("es_core_news_sm") # python -m spacy download en_core_web_sm
    fileName = os.path.basename(filePath)
    pathProcessedFiles = '../documentos/almacen/textoProcesado/'

    if (fileName.endswith('.pdf')):
        command = './pdf2txtScript.sh ' + filePath
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
    elif (fileName.endswith('.html')):
        fileText = open(filePath, "r")
        all_text = BeautifulSoup(fileText, features="html.parser").text
        with open(pathProcessedFiles + fileName + '.txt','w', encoding = 'utf-8') as fileWithoutHTML:
            # perform file operations
            fileWithoutHTML.write(all_text)

    file = open(pathProcessedFiles + fileName + ".txt", "r", encoding="utf8")
    linea = file.read()
    linea = linea.lower()

    word_tokens = word_tokenize(linea)

    stop_words = set(stopwords.words('spanish'))
    
    #  Limpia de de signos de puntuacion, y de stopwords
    clean_words = []
    for word in word_tokens:
        if word.strip() == '.' or word.strip() == '\'':
            pass
        #Se quitan signos de puntuacion
        if word in string.punctuation or word.isdigit():
            pass
        elif word not in stop_words:
            clean_words.append(word)

    # Lematizacion de las palabras
    lemmatized_words = []
    for i in range(len(clean_words)):
        sentence = nlp(clean_words[i])
        for word in sentence:
            lemmas=word.lemma_
            lemmatized_words.append(lemmas)

    appendFile = open(pathProcessedFiles + fileName.replace(".pdf","").replace(".html","") +'.txt','w', encoding="utf8")

    for word in lemmatized_words:
        appendFile.write(" "+ word)

    appendFile.close()

# preprocessingFile('../documentos/docsOriginales/arannador.pdf');