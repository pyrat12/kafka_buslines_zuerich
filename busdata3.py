from pykafka import KafkaClient                                         #KafkaClient laden, um Nachrichten an Kafka senden zu können
import json                                                             #JSON-Daten lesen und erzeugen
from datetime import datetime, UTC                                      #Aktuelle UTC-Zeit für den Timestamp laden
import uuid                                                             #Eindeutige IDs für Bus-Events erzeugen
import time                                                             #Zeitverzögerung zwischen Positionsupdates ermöglichen

#READ COORDINATES FROM GEOJSON
input_file = open('./data/bus3.json')                                   #JSON-Datei mit den Koordinaten der Buslinie 00003 öffnen
json_array = json.load(input_file)                                      #JSON-Inhalt in Python einlesen
coordinates = json_array['features'][0]['geometry']['coordinates']      #Koordinaten der dritten Busroute auslesen

#GENERATE UUID
def generate_uuid():                                                    #Funktion zur Erzeugung eindeutiger IDs definieren
    return uuid.uuid4()                                                 #Neue UUID erzeugen und zurückgeben

#KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")                            #Verbindung zum lokalen Kafka-Broker herstellen
topic = client.topics[b'busdata001']                                    #Kafka-Topic busdata001 auswählen
producer = topic.get_sync_producer()                                    #Synchronen Kafka-Producer erstellen

#CONSTRUCT MESSAGE AND SEND IT TO KAFKA
data = {}                                                               #Leeres Dictionary für die Busdaten erstellen
data['busline'] = '00003'                                               #Buslinie 00003 im Datensatz hinterlegen

def generate_checkpoint(coordinates):                                   #Funktion für das fortlaufende Abspielen der Route definieren
    i = 0                                                               #Beim ersten Koordinatenpunkt beginnen
    while i < len(coordinates):                                         #Route fortlaufend durchlaufen
        data['key'] = data['busline'] + '_' + str(generate_uuid())      #Eindeutigen Schlüssel für dieses Event erzeugen
        data['timestamp'] = str(datetime.now(UTC).replace(tzinfo=None)) #Aktuellen UTC-Zeitpunkt speichern
        data['latitude'] = coordinates[i][1]                            #Breitengrad der aktuellen Busposition speichern
        data['longitude'] = coordinates[i][0]                           #Längengrad der aktuellen Busposition speichern
        message = json.dumps(data)                                      #Dictionary in JSON umwandeln
        print(message)                                                  #Aktuelles Event im Terminal ausgeben
        producer.produce(message.encode('ascii'))                       #Event als Kafka-Nachricht versenden
        time.sleep(0.5)                                                 #0.5 Sekunden warten

        #if bus reaches last coordinate, start from beginning
        if i == len(coordinates)-1:                                     #Prüfen, ob der letzte Koordinatenpunkt erreicht wurde
            i = 0                                                       #Route wieder beim ersten Punkt starten
        else:                                                           #Falls noch weitere Punkte vorhanden sind
            i += 1                                                      #Zum nächsten Punkt wechseln

generate_checkpoint(coordinates)                                        #Simulation der Buslinie 00003 starten