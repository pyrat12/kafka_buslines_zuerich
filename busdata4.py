from pykafka import KafkaClient  # NEU: KafkaClient laden, damit auch die vierte Buslinie Daten an Kafka senden kann
import json  # NEU: JSON-Datei der neuen Busroute lesen und Nachrichten als JSON erzeugen
from datetime import datetime, UTC  # NEU: Aktuelle UTC-Zeit für die Events der vierten Buslinie laden
import uuid  # NEU: Eindeutige IDs für jedes Event der vierten Buslinie erzeugen
import time  # NEU: Zeitabstand zwischen den einzelnen Buspositionen ermöglichen

#READ COORDINATES FROM GEOJSON
input_file = open('./data/bus4.json')  # NEU: Koordinaten der neuen vierten Buslinie aus bus4.json laden
json_array = json.load(input_file)  # NEU: JSON-Datei in eine Python-Datenstruktur einlesen
coordinates = json_array['features'][0]['geometry']['coordinates']  # NEU: Koordinatenliste der vierten Busroute auslesen

#GENERATE UUID
def generate_uuid():  # NEU: Funktion zur Erzeugung einer eindeutigen Event-ID definieren
    return uuid.uuid4()  # NEU: Neue UUID für jedes Positions-Event erzeugen

#KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")  # NEU: Verbindung zum bestehenden Kafka-Broker herstellen
topic = client.topics[b'busdata001']  # NEU: Auch die vierte Buslinie sendet ihre Events an das bestehende Topic busdata001
producer = topic.get_sync_producer()  # NEU: Synchronen Kafka-Producer für die neue Buslinie erstellen

#CONSTRUCT MESSAGE AND SEND IT TO KAFKA
data = {}  # NEU: Leeres Dictionary für die Daten der vierten Buslinie erstellen
data['busline'] = '00004'  # NEU: Neue Buslinie eindeutig als 00004 kennzeichnen

def generate_checkpoint(coordinates):  # NEU: Funktion zum fortlaufenden Abspielen der neuen Busroute definieren
    i = 0  # NEU: Beim ersten Koordinatenpunkt der Route beginnen
    while i < len(coordinates):  # NEU: Alle Koordinaten der neuen Route fortlaufend durchlaufen
        data['key'] = data['busline'] + '_' + str(generate_uuid())  # NEU: Eindeutigen Schlüssel aus Buslinie 00004 und UUID erzeugen
        data['timestamp'] = str(datetime.now(UTC).replace(tzinfo=None))  # NEU: Aktuellen Zeitpunkt des Events speichern
        data['latitude'] = coordinates[i][1]  # NEU: Breitengrad der aktuellen Position übernehmen
        data['longitude'] = coordinates[i][0]  # NEU: Längengrad der aktuellen Position übernehmen
        message = json.dumps(data)  # NEU: Daten der vierten Buslinie in JSON umwandeln
        print(message)  # NEU: Gesendete Position im Terminal anzeigen
        producer.produce(message.encode('ascii'))  # NEU: Positions-Event der vierten Linie an Kafka senden
        time.sleep(0.5)  # NEU: 0.5 Sekunden bis zum nächsten Positionsupdate warten

        #if bus reaches last coordinate, start from beginning
        if i == len(coordinates)-1:  # NEU: Prüfen, ob Bus 4 das Ende seiner Route erreicht hat
            i = 0  # NEU: Route wieder von vorne beginnen
        else:  # NEU: Falls das Ende noch nicht erreicht wurde
            i += 1  # NEU: Zum nächsten Koordinatenpunkt wechseln

generate_checkpoint(coordinates)  # NEU: Simulation der vierten Buslinie starten