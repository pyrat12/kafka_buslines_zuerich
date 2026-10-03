from pykafka import KafkaClient                                     #KafkaClient laden, um Nachrichten an Kafka senden zu können
import json                                                         #JSON-Daten lesen und erzeugen
from datetime import datetime, UTC                                  #Aktuelle UTC-Zeit für den Timestamp laden
import uuid                                                         #Eindeutige IDs für Bus-Events erzeugen
import time                                                         #Pausen zwischen den Positionsupdates ermöglichen

#READ COORDINATES FROM GEOJSON
input_file = open('./data/bus2.json')                               #JSON-Datei mit den Koordinaten der Buslinie 00002 öffnen
json_array = json.load(input_file)                                  #JSON-Datei in eine Python-Datenstruktur einlesen
coordinates = json_array['features'][0]['geometry']['coordinates']  #Koordinatenliste der zweiten Busroute auslesen

#GENERATE UUID
def generate_uuid():                                                #Funktion zur Erzeugung einer eindeutigen ID definieren
    return uuid.uuid4()                                             #Neue UUID erzeugen und zurückgeben

#KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")                        #Verbindung zum Kafka-Broker herstellen
topic = client.topics[b'busdata001']                                #Kafka-Topic busdata001 auswählen
producer = topic.get_sync_producer()                                #Synchronen Kafka-Producer erstellen

#CONSTRUCT MESSAGE AND SEND IT TO KAFKA
data = {}                                                           #Leeres Dictionary für die Busdaten erstellen
data['busline'] = '00002'                                           #Buslinie 00002 im Datensatz hinterlegen

def generate_checkpoint(coordinates):                               #Funktion für das fortlaufende Abspielen der Busroute definieren
    i = 0                                                           #Beim ersten Koordinatenpunkt beginnen
    while i < len(coordinates):                                     #Solange Koordinaten vorhanden sind, Route weiter abspielen
        data['key'] = data['busline'] + '_' + str(generate_uuid())  #Eindeutigen Event-Schlüssel erzeugen
        data['timestamp'] = str(datetime.now(UTC).replace(tzinfo=None))  #Aktuellen UTC-Zeitpunkt speichern
        data['latitude'] = coordinates[i][1]                             #Breitengrad der aktuellen Position speichern
        data['longitude'] = coordinates[i][0]                            #Längengrad der aktuellen Position speichern
        message = json.dumps(data)                                       #Busdaten in einen JSON-String umwandeln
        print(message)                                                   #Nachricht zur Kontrolle im Terminal anzeigen
        producer.produce(message.encode('ascii'))                        #Nachricht an Kafka senden
        time.sleep(0.5)                                                  #0.5 Sekunden bis zur nächsten Position warten

        #if bus reaches last coordinate, start from beginning
        if i == len(coordinates)-1:                                 #Prüfen, ob das Ende der Route erreicht wurde
            i = 0                                                   #Route wieder von vorne starten
        else:                                                       #Falls das Ende noch nicht erreicht wurde
            i += 1                                                  #Zum nächsten Koordinatenpunkt wechseln

generate_checkpoint(coordinates)                                    #Simulation der Buslinie 00002 starten