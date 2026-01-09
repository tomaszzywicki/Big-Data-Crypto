from confluent_kafka import Producer
import time
import json

conf = {"bootstrap.servers": "localhost:9092", "client.id": "python-producer-simple"}

producer = Producer(conf)

TOPIC = "connect-test"


def delivery_report(err, msg):
    """Wywoływana po potwierdzeniu dostarczenia wiadomości lub wystąpieniu błędu"""
    if err is not None:
        print(f"[!] Błąd dostarczenia wiadomości: {err}")
    else:
        print(f"[+] Dostarczono do {msg.topic()} [partycja: {msg.partition()}] offset: {msg.offset()}")


def run_producer():
    print(f"[*] Uruchamiam producenta. Wysyłanie do tematu: {TOPIC}")

    try:
        for i in range(1000):

            data = {"id": i, "message": f"Element nr {i}", "status": "OK"}

            value_bytes = json.dumps(data).encode("utf-8")

            key_bytes = str(i).encode("utf-8")

            producer.produce(topic=TOPIC, key=key_bytes, value=value_bytes, on_delivery=delivery_report)

            # 5. Poll (Ważne!)
            # Ta metoda służy do obsługi zdarzeń zwrotnych (callbacków).
            # Mówi producentowi: "Sprawdź, czy przyszły jakieś potwierdzenia od brokera i jeśli tak, uruchom delivery_report".
            # Argument '0' oznacza, że nie czekamy (non-blocking), sprawdzamy i lecimy dalej.
            producer.poll(0)

            print(f"[*] Wysłano element {i} (czekam na potwierdzenie...)")

            # Symulacja odstępu czasu (5 sekund)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[!] Przerwano przez użytkownika.")

    finally:
        # 6. Flush (Spłukanie bufora)
        # Bardzo ważne na koniec! Czeka, aż wszystkie wiadomości z bufora zostaną fizycznie wysłane do brokera.
        # Bez tego, po naciśnięciu Ctrl+C, ostatnie wiadomości mogłyby zostać zgubione.
        print("Czekanie na wysłanie pozostałych wiadomości...")
        producer.flush()


if __name__ == "__main__":
    run_producer()
