from flask import Flask, render_template, Response              #Flask-Komponenten laden: Web-App erstellen, HTML laden und Streaming-Antworten senden
from pykafka import KafkaClient                                 #KafkaClient laden, um eine Verbindung zum Kafka-Broker herzustellen
from pykafka.common import OffsetType                           #OffsetType laden, um festzulegen, ab welcher Kafka-Nachricht gelesen wird

def get_kafka_client():                                         #Funktion definieren, welche eine Kafka-Verbindung erstellt
    return KafkaClient(hosts='localhost:9092')                  #Verbindung zum lokalen Kafka-Broker auf Port 9092 zurückgeben

app = Flask(__name__)                                           #Flask-Webanwendung erstellen

@app.route('/')                                                 #URL "/" mit der folgenden Funktion verbinden
def index():                                                    #Funktion für die Startseite definieren
    return render_template('index.html')                        #HTML-Datei index.html aus dem templates-Ordner laden

# Consumer API
@app.route('/topic/<topicname>')                                #Dynamische URL erstellen, über die ein bestimmtes Kafka-Topic konsumiert werden kann
def get_messages(topicname):                                    #Funktion definieren, welche Nachrichten des angegebenen Topics liefert
    client = get_kafka_client()                                 #Verbindung zum Kafka-Broker herstellen
    
    # Convert the topic name to a byte string
    topicname_bytes = topicname.encode('utf-8')                 #Topic-Namen von String in Bytes umwandeln, da PyKafka Bytes erwartet

    def events():                                               #Generator-Funktion definieren, welche fortlaufend Kafka-Nachrichten an den Browser liefert
        # Without this, each new SSE connection has no consumer group to
        # resume from, so pykafka defaults to replaying the topic from the
        # earliest offset - a new browser tab would see the full backlog of
        # past positions instead of jumping straight to live ones.
        consumer = client.topics[topicname_bytes].get_simple_consumer(  #Consumer für das gewünschte Kafka-Topic erstellen
            auto_offset_reset=OffsetType.LATEST,                        #Beim neuesten verfügbaren Kafka-Event beginnen
            reset_offset_on_start=True,                                 #Offset beim Start zurücksetzen, damit direkt aktuelle Daten gelesen werden
        )
        for i in consumer:                                              #Fortlaufend über alle neu eintreffenden Kafka-Nachrichten iterieren
            if i.value is not None:                                     #Prüfen, ob die empfangene Kafka-Nachricht tatsächlich Daten enthält
                try:                                                    #Versuchen, die Nachricht zu dekodieren und an den Browser weiterzugeben
                    yield f'data:{i.value.decode("utf-8")}\n\n'         #Kafka-Bytes in Text umwandeln und als Server-Sent Event an den Browser senden
                except UnicodeDecodeError as e:                         #Fehler abfangen, falls die Nachricht nicht als UTF-8 dekodiert werden kann
                    print(f"Error decoding message: {e}")               #Fehlermeldung im Terminal ausgeben
                    continue                                            #Fehlerhafte Nachricht überspringen und mit der nächsten weitermachen

    return Response(events(), mimetype="text/event-stream")             #permanenten SSE-Datenstrom als HTTP-Antwort an den Browser zurückgeben

if __name__ == '__main__':                                              #Prüfen, ob app.py direkt ausgeführt wurde
    app.run(debug=True, port=5001)                                      #Flask-Webserver im Debug-Modus auf Port 5001 starten