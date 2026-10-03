from pykafka import KafkaClient                                             #KafkaClient laden, um Nachrichten an Kafka senden zu können
import json                                                                 #JSON-Daten lesen und Python-Daten wieder in JSON umwandeln
from datetime import datetime, UTC                                          #Aktuelle UTC-Zeit für den Timestamp laden
import uuid                                                                 #Eindeutige IDs für einzelne Bus-Events erzeugen
import time                                                                 #Pausen zwischen den gesendeten Buspositionen ermöglichen

#READ COORDINATES FROM GEOJSON
input_file = open('./data/bus1.json')                                       #JSON-Datei mit den Koordinaten der Buslinie 00001 öffnen
json_array = json.load(input_file)                                          #JSON-Datei in eine Python-Datenstruktur einlesen
coordinates = json_array['features'][0]['geometry']['coordinates']          #Koordinatenliste der Busroute aus dem GeoJSON auslesen

#GENERATE UUID
def generate_uuid():                                                        #Funktion zur Erzeugung einer eindeutigen ID definieren
    return uuid.uuid4()                                                     #Neue zufällige UUID erzeugen und zurückgeben

#KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")                                #Verbindung zum lokalen Kafka-Broker auf Port 9092 herstellen
topic = client.topics[b'busdata001']                                        #Kafka-Topic busdata001 auswählen
producer = topic.get_sync_producer()                                        #Synchronen Producer erstellen, der Nachrichten an dieses Topic sendet

#CONSTRUCT MESSAGE AND SEND IT TO KAFKA
data = {}                                                                   #Leeres Dictionary für die Busdaten erstellen
data['busline'] = '00001'                                                   #Buslinie 00001 im Datensatz hinterlegen

def generate_checkpoint(coordinates):                                       #Funktion definieren, welche alle Koordinaten der Busroute fortlaufend verarbeitet
    i = 0                                                                   #Index beim ersten Koordinatenpunkt starten
    while i < len(coordinates):                                             #Schleife ausführen, solange Koordinaten vorhanden sind
        data['key'] = data['busline'] + '_' + str(generate_uuid())          #Eindeutigen Schlüssel aus Buslinie und UUID erstellen
        data['timestamp'] = str(datetime.now(UTC).replace(tzinfo=None))     #Aktuellen UTC-Zeitpunkt des Events speichern
        data['latitude'] = coordinates[i][1]                                #Breitengrad des aktuellen Koordinatenpunktes übernehmen
        data['longitude'] = coordinates[i][0]                               #Längengrad des aktuellen Koordinatenpunktes übernehmen
        message = json.dumps(data)                                          #Python-Dictionary in einen JSON-String umwandeln
        print(message)                                                      #Gesendete Busposition im Terminal anzeigen
        producer.produce(message.encode('ascii'))                           #JSON-Nachricht in Bytes umwandeln und an Kafka senden
        time.sleep(0.5)                                                     #0.5 Sekunden bis zur nächsten Busposition warten

        #if bus reaches last coordinate, start from beginning
        if i == len(coordinates)-1:                                         #Prüfen, ob der Bus den letzten Punkt seiner Route erreicht hat
            i = 0                                                           #Wieder beim ersten Koordinatenpunkt beginnen
        else:                                                               #Falls noch weitere Koordinaten vorhanden sind
            i += 1                                                          #Zum nächsten Koordinatenpunkt wechseln

generate_checkpoint(coordinates)                                            #Simulation der Buslinie starten