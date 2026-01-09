# 📚 PySpark Structured Streaming - Kompletny Przewodnik

> **Wersja:** PySpark 4.0.1  
> **Autor:** Big-Data-Crypto Project  
> **Data:** Grudzień 2025

---

## 📖 Spis treści

1. [Wprowadzenie](#1-wprowadzenie)
2. [SparkSession - punkt wejścia](#2-sparksession---punkt-wejścia)
3. [Źródła danych (Sources)](#3-źródła-danych-sources)
4. [Transformacje i operacje na strumieniach](#4-transformacje-i-operacje-na-strumieniach)
5. [Window Operations - agregacje czasowe](#5-window-operations---agregacje-czasowe)
6. [Watermarks - zarządzanie opóźnieniami](#6-watermarks---zarządzanie-opóźnieniami)
7. [Stateful Processing - przetwarzanie stanowe](#7-stateful-processing---przetwarzanie-stanowe)
8. [Output Sinks - zapis wyników](#8-output-sinks---zapis-wyników)
9. [Triggery - kontrola przetwarzania](#9-triggery---kontrola-przetwarzania)
10. [Checkpointing - odporność na awarie](#10-checkpointing---odporność-na-awarie)
11. [Monitorowanie i debugowanie](#11-monitorowanie-i-debugowanie)
12. [Best Practices i wzorce](#12-best-practices-i-wzorce)
13. [Kompletne przykłady end-to-end](#13-kompletne-przykłady-end-to-end)
14. [Troubleshooting - najczęstsze problemy](#14-troubleshooting---najczęstsze-problemy)

---

## 1. Wprowadzenie

### 1.1 Czym jest Structured Streaming?

Structured Streaming to silnik przetwarzania strumieniowego zbudowany na Spark SQL. Kluczowa idea: **traktuj strumień danych jak nieskończoną tabelę**, do której ciągle dopisywane są nowe wiersze.

```
Dane przychodzące:     [msg1] [msg2] [msg3] [msg4] ...
                          ↓      ↓      ↓      ↓
                       ┌─────────────────────────────┐
Nieskończona tabela:   │ msg1                        │  ← Batch 0
                       │ msg2                        │
                       ├─────────────────────────────┤
                       │ msg3                        │  ← Batch 1
                       │ msg4                        │
                       ├─────────────────────────────┤
                       │ ...                         │  ← Batch N
                       └─────────────────────────────┘
```

### 1.2 Architektura przepływu danych

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│   SOURCE     │ ──► │  TRANSFORMACJE   │ ──► │    SINK      │
│ (Kafka/File) │     │ (filter/map/agg) │     │ (console/DB) │
└──────────────┘     └──────────────────┘     └──────────────┘
       │                      │                       │
       ▼                      ▼                       ▼
  readStream              DataFrame              writeStream
```

### 1.3 Kluczowe pojęcia - słownik

| Pojęcie | Opis | Przykład |
|---------|------|----------|
| **Source** | Skąd czytamy dane | Kafka, pliki, socket |
| **Sink** | Gdzie zapisujemy wyniki | console, pliki, Kafka, baza danych |
| **Trigger** | Jak często przetwarzamy mikro-batch | co 10 sekund, ciągle, raz |
| **Watermark** | Jak długo czekamy na spóźnione dane | 10 minut tolerancji |
| **Checkpoint** | Gdzie zapisujemy stan do recovery | katalog na HDFS/lokalny |
| **Output Mode** | Jak emitujemy wyniki | append/update/complete |
| **Event Time** | Czas zdarzenia (z danych) | timestamp w JSON |
| **Processing Time** | Czas przetwarzania (zegar Sparka) | kiedy Spark przetwarza |

### 1.4 Minimalny działający przykład

```python
from pyspark.sql import SparkSession

# 1. Utwórz sesję Spark
spark = SparkSession.builder.appName("MinimalStream").getOrCreate()

# 2. Zdefiniuj źródło (readStream)
lines = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# 3. Transformacje (opcjonalne)
# lines to DataFrame, można użyć filter, select, groupBy, itp.

# 4. Zdefiniuj sink i uruchom (writeStream)
query = (
    lines.writeStream
    .format("console")
    .start()
)

# 5. Czekaj na zakończenie
query.awaitTermination()
```

---

## 2. SparkSession - punkt wejścia

### 2.1 Tworzenie sesji - podstawy

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MojaAplikacja")           # Nazwa widoczna w Spark UI
    .master("local[*]")                  # Tryb uruchomienia
    .config("klucz", "wartość")          # Dodatkowe konfiguracje
    .getOrCreate()                       # Utwórz lub użyj istniejącej
)
```

### 2.2 Tryby uruchomienia (`master`)

| Wartość | Opis | Kiedy używać |
|---------|------|--------------|
| `local` | 1 wątek, lokalnie | Debugowanie krok po kroku |
| `local[*]` | Wszystkie rdzenie, lokalnie | Lokalne testy wydajnościowe |
| `local[4]` | 4 wątki, lokalnie | Kontrolowane testy równoległości |
| `spark://host:7077` | Standalone cluster | Produkcja na klastrze Spark |
| `yarn` | YARN cluster manager | Produkcja na Hadoop |
| `k8s://...` | Kubernetes | Produkcja na K8s |

### 2.3 Konfiguracja dla Kafka

```python
SPARK_VERSION = "4.0.1"
SCALA_VERSION = "2.13"  # Sprawdź wersję Scala w Twoim Sparku!

KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"

spark = (
    SparkSession.builder
    .appName("KafkaStreaming")
    .master("local[*]")
    
    # Pakiet Kafka - WYMAGANE
    .config("spark.jars.packages", KAFKA_PACKAGE)
    
    # Optymalizacje dla małych danych
    .config("spark.sql.shuffle.partitions", "4")
    
    # Graceful shutdown
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    
    # Adaptive Query Execution (AQE) - optymalizacja w runtime
    .config("spark.sql.adaptive.enabled", "true")
    
    .getOrCreate()
)
```

### 2.4 Poziomy logowania

```python
# Ustaw PRZED rozpoczęciem przetwarzania
spark.sparkContext.setLogLevel("ERROR")   # Tylko błędy (czyste logi)
spark.sparkContext.setLogLevel("WARN")    # Ostrzeżenia + błędy
spark.sparkContext.setLogLevel("INFO")    # Domyślne (dużo logów)
spark.sparkContext.setLogLevel("DEBUG")   # Wszystko (bardzo dużo)

# Rekomendacja: "ERROR" lub "WARN" dla developmentu
# "INFO" dla produkcji (monitoring)
```

### 2.5 Sprawdzanie wersji i konfiguracji

```python
# Wersja Spark
print(f"Spark version: {spark.version}")

# Aktualna konfiguracja
for item in spark.sparkContext.getConf().getAll():
    print(f"{item[0]} = {item[1]}")

# Sprawdzenie czy Kafka package jest załadowany
print(spark.sparkContext.getConf().get("spark.jars.packages"))
```

---

## 3. Źródła danych (Sources)

### 3.1 Kafka Source - najważniejsze źródło

#### Podstawowe użycie

```python
kafkaStream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "my-topic")
    .load()
)
```

#### Pełna lista opcji Kafka Source

| Opcja | Wartości | Opis |
|-------|----------|------|
| `kafka.bootstrap.servers` | `host:port,host2:port2` | Adresy brokerów Kafka (wymagane) |
| `subscribe` | `topic1,topic2` | Subskrypcja konkretnych topiców |
| `subscribePattern` | `topic-.*` | Subskrypcja przez regex |
| `assign` | `{"topic1":[0,1]}` | Przypisanie konkretnych partycji |
| `startingOffsets` | `earliest`, `latest`, JSON | Skąd zacząć czytać |
| `endingOffsets` | `latest`, JSON | Gdzie skończyć (batch mode) |
| `failOnDataLoss` | `true`/`false` | Czy failować przy utracie danych |
| `maxOffsetsPerTrigger` | liczba | Max rekordów na trigger |
| `minPartitions` | liczba | Min partycji do czytania |

#### startingOffsets - szczegóły

```python
# Czytaj od początku topicu (wszystkie historyczne dane)
.option("startingOffsets", "earliest")

# Czytaj tylko nowe wiadomości (od momentu startu)
.option("startingOffsets", "latest")

# Czytaj od konkretnych offsetów (JSON)
.option("startingOffsets", '{"topic1":{"0":100,"1":200}}')
```

#### Schemat danych z Kafka

Kafka Source **zawsze** zwraca DataFrame z następującymi kolumnami:

| Kolumna | Typ | Opis |
|---------|-----|------|
| `key` | binary | Klucz wiadomości (null jeśli brak) |
| `value` | binary | Wartość wiadomości (Twoje dane!) |
| `topic` | string | Nazwa topicu |
| `partition` | int | Numer partycji |
| `offset` | long | Offset wiadomości |
| `timestamp` | timestamp | Timestamp Kafka |
| `timestampType` | int | Typ timestampu (0=create, 1=append) |

#### Przykład pełnej konfiguracji Kafka

```python
kafkaStream = (
    spark.readStream
    .format("kafka")
    
    # Połączenie
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    
    # Subskrypcja
    .option("subscribe", "crypto-prices,crypto-posts")
    
    # Offsety
    .option("startingOffsets", "latest")
    
    # Kontrola przepływu
    .option("maxOffsetsPerTrigger", "10000")
    
    # Bezpieczeństwo (opcjonalne)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    
    # Obsługa błędów
    .option("failOnDataLoss", "false")
    
    .load()
)
```

### 3.2 File Source - czytanie plików

#### Podstawowe formaty

```python
# JSON
jsonStream = (
    spark.readStream
    .format("json")
    .schema(mySchema)  # WYMAGANE dla streamingu!
    .option("path", "/data/input/json/")
    .load()
)

# CSV
csvStream = (
    spark.readStream
    .format("csv")
    .schema(mySchema)
    .option("path", "/data/input/csv/")
    .option("header", "true")
    .load()
)

# Parquet (schemat inferowany automatycznie)
parquetStream = (
    spark.readStream
    .format("parquet")
    .option("path", "/data/input/parquet/")
    .load()
)

# Tekst (linia = rekord)
textStream = (
    spark.readStream
    .format("text")
    .option("path", "/data/input/text/")
    .load()
)
```

#### Opcje File Source

| Opcja | Opis |
|-------|------|
| `path` | Ścieżka do katalogu (nie pliku!) |
| `maxFilesPerTrigger` | Max plików na trigger |
| `latestFirst` | Czy przetwarzać najpierw najnowsze |
| `fileNameOnly` | Używaj tylko nazwy pliku (nie ścieżki) |
| `cleanSource` | Co robić z przetworzonymi plikami (`archive`, `delete`) |

### 3.3 Socket Source - do testów

```python
# UWAGA: Tylko do developmentu! Nie gwarantuje fault-tolerance
socketStream = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Zwraca DataFrame z jedną kolumną "value" (string)
```

### 3.4 Rate Source - generowanie danych testowych

```python
# Generuje rekordy z rosnącym timestampem
rateStream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 100)      # 100 rekordów/s
    .option("numPartitions", 4)         # 4 partycje
    .load()
)

# Zwraca: timestamp (timestamp), value (long - licznik)
```

---

## 4. Transformacje i operacje na strumieniach

### 4.1 Podstawowe transformacje (identyczne jak batch)

```python
from pyspark.sql.functions import col, lit, when, concat, upper, lower

# Selekcja kolumn
df = stream.select("col1", "col2", "col3")
df = stream.select(col("col1"), col("col2").alias("new_name"))

# Filtrowanie
df = stream.filter(col("price") > 100)
df = stream.filter("price > 100")  # SQL syntax
df = stream.where(col("currency") == "bitcoin")

# Dodawanie kolumn
df = stream.withColumn("price_usd", col("price") * 1.0)
df = stream.withColumn("is_expensive", when(col("price") > 1000, True).otherwise(False))

# Usuwanie kolumn
df = stream.drop("unwanted_column")

# Zmiana nazwy
df = stream.withColumnRenamed("old_name", "new_name")
```

### 4.2 Parsowanie JSON z Kafka

```python
from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Definicja schematu JSON
PRICE_SCHEMA = StructType([
    StructField("currency", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("timestamp", StringType(), nullable=True),
])

# Parsowanie
df = (
    kafkaStream
    # 1. Zamień bytes na string
    .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS json_value")
    
    # 2. Sparsuj JSON do struktury
    .select(
        col("key"),
        from_json(col("json_value"), PRICE_SCHEMA).alias("data")
    )
    
    # 3. "Rozpakuj" strukturę do kolumn
    .select(
        col("key"),
        col("data.currency"),
        col("data.price"),
        col("data.timestamp")
    )
)
```

#### Alternatywna składnia (bardziej zwięzła)

```python
df = (
    kafkaStream
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .select(
        col("key"),
        from_json(col("value"), PRICE_SCHEMA).alias("v")
    )
    .select("key", "v.*")  # v.* rozpakuje wszystkie pola struktury
)
```

### 4.3 Konwersja typów i formatowanie

```python
from pyspark.sql.functions import to_timestamp, to_date, date_format, round

# String → Timestamp
df = df.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# String → Date
df = df.withColumn("event_date", to_date(col("timestamp")))

# Formatowanie daty
df = df.withColumn("formatted", date_format(col("event_time"), "dd-MM-yyyy HH:mm"))

# Zaokrąglanie
df = df.withColumn("price_rounded", round(col("price"), 2))
```

### 4.4 Agregacje podstawowe (bez okien)

```python
from pyspark.sql.functions import count, sum, avg, min, max, approx_count_distinct

# UWAGA: Agregacje bez groupBy wymagają output mode "complete"
aggregated = (
    df.groupBy("currency")
    .agg(
        count("*").alias("total_count"),
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        sum("price").alias("total_volume"),
        approx_count_distinct("key").alias("unique_keys")
    )
)
```

### 4.5 Łączenie strumieni (Joins)

#### Stream-Stream Join

```python
# Dwa strumienie: ceny i wolumeny
pricesStream = ...  # zawiera: currency, price, timestamp
volumesStream = ... # zawiera: currency, volume, timestamp

# Join z watermarkiem (WYMAGANE dla stream-stream)
joined = (
    pricesStream
    .withWatermark("timestamp", "10 minutes")
    .join(
        volumesStream.withWatermark("timestamp", "10 minutes"),
        on=["currency"],
        how="inner"
    )
)
```

#### Stream-Static Join

```python
# Statyczna tabela (np. metadane)
currencyMeta = spark.read.json("currency_metadata.json")

# Join streamu ze statyczną tabelą (proste, bez watermarków)
enriched = stream.join(currencyMeta, on="currency", how="left")
```

### 4.6 Deduplikacja

```python
# Usuń duplikaty w oknie czasowym
deduplicated = (
    df
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["key", "currency"])  # kolumny do deduplikacji
)
```

---

## 5. Window Operations - agregacje czasowe

### 5.1 Koncept okien czasowych

```
Timeline:  00:00    00:05    00:10    00:15    00:20
              |--------|--------|--------|--------|
              
Tumbling   [  Window 1  ][  Window 2  ][  Window 3  ]
(5 min)    
              
Sliding    [    Window 1    ]
(10min/5min)     [    Window 2    ]
                      [    Window 3    ]
```

### 5.2 Tumbling Window (okna nieprzekrywające się)

```python
from pyspark.sql.functions import window, avg, count

# Średnia cena w 5-minutowych oknach
windowed = (
    df
    .groupBy(
        window(col("event_time"), "5 minutes"),  # okno 5 min
        col("currency")
    )
    .agg(
        avg("price").alias("avg_price"),
        count("*").alias("count")
    )
)

# Wynik zawiera kolumnę "window" ze strukturą: {start, end}
# Możesz ją "rozpakować":
result = windowed.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("currency"),
    col("avg_price"),
    col("count")
)
```

### 5.3 Sliding Window (okna przesuwne/nakładające się)

```python
# Średnia krocząca: okno 10 minut, przesuwane co 2 minuty
# Każdy rekord może należeć do wielu okien!
sliding = (
    df
    .groupBy(
        window(col("event_time"), "10 minutes", "2 minutes"),
        col("currency")
    )
    .agg(avg("price").alias("moving_avg"))
)
```

#### Wizualizacja Sliding Window

```
Dane:     |--A--|--B--|--C--|--D--|--E--|
Czas:     00:00 00:02 00:04 00:06 00:08 00:10

Window 1: [A, B, C, D, E]     (00:00 - 00:10)
Window 2:       [B, C, D, E, ...] (00:02 - 00:12)
Window 3:             [C, D, E, ...] (00:04 - 00:14)
```

### 5.4 Session Window (okna sesyjne)

```python
from pyspark.sql.functions import session_window

# Grupuj rekordy w sesje - nowa sesja jeśli przerwa > 5 minut
sessioned = (
    df
    .groupBy(
        session_window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(count("*").alias("events_in_session"))
)
```

### 5.5 Praktyczne przykłady agregacji okienkowych

#### Średnia, min, max w oknie

```python
price_stats = (
    df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("currency")
    )
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("sample_count"),
        (max("price") - min("price")).alias("price_range")
    )
)
```

#### Wykrywanie skoków ceny

```python
from pyspark.sql.functions import first, last, abs as spark_abs

price_change = (
    df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("currency")
    )
    .agg(
        first("price").alias("open_price"),
        last("price").alias("close_price"),
        min("price").alias("low"),
        max("price").alias("high")
    )
    .withColumn(
        "change_percent",
        ((col("close_price") - col("open_price")) / col("open_price") * 100)
    )
    .withColumn(
        "is_spike",
        spark_abs(col("change_percent")) > 5  # >5% zmiana = spike
    )
)
```

---

## 6. Watermarks - zarządzanie opóźnieniami

### 6.1 Po co są watermarki?

W rzeczywistych systemach dane mogą przychodzić **z opóźnieniem** (out-of-order). Watermark mówi Sparkowi:

> "Czekaj na dane spóźnione maksymalnie o X czasu. Starsze dane ignoruj."

Bez watermarków Spark musiałby przechowywać stan **w nieskończoność**.

### 6.2 Jak działa watermark

```
Event Time:    |--A(00:00)--|--C(00:07)--|--B(00:03)--|--D(00:12)--|
                     ↓            ↓             ↓            ↓
Processing:       00:01        00:08         00:09        00:13

Watermark "5 minutes" przy przetwarzaniu D (00:13):
  - Watermark = 00:13 - 5min = 00:08
  - Event B (00:03) jest PRZED watermarkiem → ODRZUCONY
  - Event A (00:00) jest PRZED watermarkiem → ODRZUCONY
```

### 6.3 Użycie watermarków

```python
# ZAWSZE definiuj watermark PRZED groupBy/window!
windowed = (
    df
    .withWatermark("event_time", "10 minutes")  # tolerancja 10 min
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("currency")
    )
    .agg(avg("price").alias("avg_price"))
)
```

### 6.4 Wybór wartości watermarku

| Scenariusz | Wartość | Uzasadnienie |
|------------|---------|--------------|
| Dane real-time, niskie opóźnienia | 1-5 min | Szybkie wyniki, mało spóźnionych |
| IoT, czasem problemy z siecią | 10-30 min | Tolerancja na reconnect |
| Batch-like, dane z różnych źródeł | 1-24h | Duża tolerancja |
| Dane historyczne | brak (batch) | Wszystko jest "spóźnione" |

### 6.5 Watermark a output mode

| Output Mode | Kiedy emituje wyniki |
|-------------|---------------------|
| `append` | Gdy okno jest "zamknięte" (watermark przeszedł) |
| `update` | Przy każdym triggerze (nawet niezamknięte okna) |
| `complete` | Cała tabela wynikowa (tylko bez watermarków) |

---

## 7. Stateful Processing - przetwarzanie stanowe

### 7.1 Kiedy potrzebujesz przetwarzania stanowego

- Zliczanie unikalnych użytkowników w czasie
- Moving average po ostatnich N próbkach (nie czasowe)
- Wykrywanie wzorców w sekwencji zdarzeń
- Custom logika akumulacji

### 7.2 mapGroupsWithState - niskopoziomowe API

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout

def update_state(key, values, state: GroupState):
    """
    key: klucz grupy (np. currency)
    values: Iterator nowych wartości
    state: obiekt stanu (może być pusty/istniejący)
    """
    # Pobierz istniejący stan lub zainicjuj
    if state.exists:
        current = state.get
    else:
        current = {"count": 0, "sum": 0.0}
    
    # Aktualizuj stan
    for value in values:
        current["count"] += 1
        current["sum"] += value.price
    
    # Zapisz stan
    state.update(current)
    
    # Zwróć wynik
    avg = current["sum"] / current["count"] if current["count"] > 0 else 0
    return (key, current["count"], avg)

# Użycie (wymaga Scala/Java UDF wrapper w PySpark 4.x)
# W praktyce łatwiejsze jest użycie applyInPandasWithState
```

### 7.3 applyInPandasWithState - Pandas UDF dla stanu (PySpark 3.4+)

```python
from pyspark.sql.streaming import GroupState
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import pandas as pd

# Schemat stanu
STATE_SCHEMA = StructType([
    StructField("count", LongType()),
    StructField("sum", DoubleType()),
])

# Schemat wyjścia
OUTPUT_SCHEMA = StructType([
    StructField("currency", StringType()),
    StructField("running_avg", DoubleType()),
    StructField("total_count", LongType()),
])

def update_running_average(
    key: tuple,
    pdf_iter,  # Iterator[pd.DataFrame]
    state: GroupState
) -> pd.DataFrame:
    """Oblicz średnią kroczącą per currency."""
    
    currency = key[0]
    
    # Pobierz stan
    if state.exists:
        current = state.get
        count = current[0]
        total = current[1]
    else:
        count = 0
        total = 0.0
    
    # Przetwórz nowe dane
    for pdf in pdf_iter:
        count += len(pdf)
        total += pdf["price"].sum()
    
    # Zapisz stan
    state.update((count, total))
    
    # Zwróć wynik
    avg = total / count if count > 0 else 0.0
    return pd.DataFrame({
        "currency": [currency],
        "running_avg": [avg],
        "total_count": [count]
    })

# Użycie
result = (
    df
    .groupBy("currency")
    .applyInPandasWithState(
        update_running_average,
        OUTPUT_SCHEMA,
        STATE_SCHEMA,
        "append",
        GroupStateTimeout.NoTimeout
    )
)
```

---

## 8. Output Sinks - zapis wyników

### 8.1 Console Sink (debugowanie)

```python
query = (
    df.writeStream
    .format("console")
    .option("truncate", "false")     # Nie obcinaj długich wartości
    .option("numRows", 50)            # Pokaż max 50 wierszy
    .outputMode("append")
    .start()
)
```

### 8.2 File Sink (zapis do plików)

```python
# JSON
query = (
    df.writeStream
    .format("json")
    .option("path", "/output/json/")
    .option("checkpointLocation", "/checkpoint/json/")
    .outputMode("append")  # TYLKO append dla file sink!
    .start()
)

# Parquet (lepszy dla dużych danych)
query = (
    df.writeStream
    .format("parquet")
    .option("path", "/output/parquet/")
    .option("checkpointLocation", "/checkpoint/parquet/")
    .partitionBy("currency", "date")  # Partycjonowanie!
    .start()
)

# CSV
query = (
    df.writeStream
    .format("csv")
    .option("path", "/output/csv/")
    .option("checkpointLocation", "/checkpoint/csv/")
    .option("header", "true")
    .start()
)
```

### 8.3 Kafka Sink

```python
# Wymaga kolumn: key (opcjonalnie), value (wymagane), topic (opcjonalnie)
output = df.select(
    col("currency").alias("key"),
    to_json(struct("currency", "price", "timestamp")).alias("value")
)

query = (
    output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "processed-prices")
    .option("checkpointLocation", "/checkpoint/kafka/")
    .start()
)
```

### 8.4 Memory Sink (Jupyter/interaktywne)

```python
# Zapisuje do tabeli w pamięci - idealny do notebooków
query = (
    df.writeStream
    .format("memory")
    .queryName("prices_table")
    .outputMode("append")  # lub "complete" dla agregacji
    .start()
)

# Odpytywanie (w innej komórce)
spark.sql("SELECT * FROM prices_table ORDER BY timestamp DESC LIMIT 10").show()

# Zatrzymanie
query.stop()
```

### 8.5 ForeachBatch Sink (custom logika)

```python
def write_to_database(batch_df, batch_id):
    """Wywoływane dla każdego mikro-batcha."""
    print(f"Processing batch {batch_id} with {batch_df.count()} rows")
    
    # Zapis do bazy (np. PostgreSQL)
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/db") \
        .option("dbtable", "crypto_prices") \
        .option("user", "user") \
        .option("password", "pass") \
        .mode("append") \
        .save()

query = (
    df.writeStream
    .foreachBatch(write_to_database)
    .option("checkpointLocation", "/checkpoint/db/")
    .start()
)
```

### 8.6 Foreach Sink (per-record processing)

```python
class PriceProcessor:
    """Procesor wywoływany dla każdego rekordu."""
    
    def open(self, partition_id, epoch_id):
        """Wywoływane raz na partycję."""
        print(f"Opening partition {partition_id}, epoch {epoch_id}")
        return True  # True = kontynuuj, False = pomiń partycję
    
    def process(self, row):
        """Wywoływane dla każdego wiersza."""
        print(f"Processing: {row.currency} = ${row.price}")
        # Tu możesz wysłać do API, zapisać do Redis, itp.
    
    def close(self, error):
        """Wywoływane na końcu partycji."""
        if error:
            print(f"Error: {error}")

query = (
    df.writeStream
    .foreach(PriceProcessor())
    .option("checkpointLocation", "/checkpoint/foreach/")
    .start()
)
```

### 8.7 Output Modes - porównanie

| Mode | Opis | Wspierane operacje |
|------|------|-------------------|
| `append` | Tylko nowe wiersze | Proste transformacje, window z watermark |
| `update` | Tylko zmienione wiersze | Agregacje (w tym bez watermark) |
| `complete` | Cała tabela wynikowa | Agregacje (tylko bez watermark lub z complete agg) |

```python
# Append - domyślny, dla prostych transformacji
.outputMode("append")

# Update - dla agregacji ze zmianami
.outputMode("update")

# Complete - dla pełnej tabeli agregacji
.outputMode("complete")
```

---

## 9. Triggery - kontrola przetwarzania

### 9.1 Typy triggerów

```python
from pyspark.sql.streaming import Trigger

# 1. Default (jak najszybciej)
.trigger(Trigger.ProcessingTime("0 seconds"))

# 2. Fixed interval (co X czasu)
.trigger(Trigger.ProcessingTime("10 seconds"))
.trigger(Trigger.ProcessingTime("1 minute"))

# 3. Once (przetworz raz i zakończ) - dobry do backfill
.trigger(Trigger.Once())

# 4. Available Now (przetworz wszystko dostępne, zakończ)
.trigger(Trigger.AvailableNow())

# 5. Continuous (eksperymentalny, ultra-low latency)
.trigger(Trigger.Continuous("1 second"))
```

### 9.2 Wybór triggera

| Scenariusz | Trigger | Uzasadnienie |
|------------|---------|--------------|
| Niskie latency | `ProcessingTime("0 seconds")` | ASAP processing |
| Balans latency/throughput | `ProcessingTime("10 seconds")` | Batch co 10s |
| Oszczędność zasobów | `ProcessingTime("1 minute")` | Rzadsze batche |
| Backfill historycznych | `AvailableNow()` | Przetwórz i zakończ |
| Ad-hoc processing | `Once()` | Jednorazowe uruchomienie |

### 9.3 Przykład z triggerem

```python
query = (
    df.writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime("30 seconds"))  # Co 30 sekund
    .option("truncate", "false")
    .start()
)
```

---

## 10. Checkpointing - odporność na awarie

### 10.1 Po co checkpointy?

Checkpointy zapisują:
- **Offsety** źródeł (gdzie skończyliśmy czytać)
- **Stan** agregacji (window counts, running totals)
- **Metadane** query

Dzięki temu po restarcie Spark **wznawia od miejsca awarii**, nie od początku.

### 10.2 Konfiguracja checkpointów

```python
query = (
    df.writeStream
    .format("json")
    .option("path", "/output/data/")
    .option("checkpointLocation", "/checkpoint/my-query/")  # WYMAGANE
    .start()
)
```

### 10.3 Struktura katalogu checkpoint

```
/checkpoint/my-query/
├── metadata              # Metadane query
├── offsets/              # Offsety źródeł per batch
│   ├── 0
│   ├── 1
│   └── ...
├── commits/              # Commitowane batche
│   ├── 0
│   └── ...
├── sources/              # Stan źródeł
│   └── 0/
│       └── ...
└── state/                # Stan agregacji (jeśli jest)
    └── 0/
        └── ...
```

### 10.4 Best practices dla checkpointów

```python
# 1. Używaj HDFS/S3 w produkcji (nie lokalny filesystem)
.option("checkpointLocation", "hdfs:///checkpoints/my-query/")
.option("checkpointLocation", "s3a://bucket/checkpoints/my-query/")

# 2. Osobny katalog per query
# ❌ ŹLE: /checkpoint/
# ✅ DOBRZE: /checkpoint/price-aggregation-v1/

# 3. Wersjonuj przy zmianach schematu
# v1: /checkpoint/prices-v1/
# v2: /checkpoint/prices-v2/  (po zmianie logiki)
```

---

## 11. Monitorowanie i debugowanie

### 11.1 StreamingQuery API

```python
query = df.writeStream.format("console").start()

# Status query
print(query.status)
# {'message': 'Processing new data', 'isDataAvailable': True, ...}

# Czy query jest aktywne?
print(query.isActive)  # True/False

# Ostatni progress
print(query.lastProgress)
# {'id': '...', 'runId': '...', 'batchId': 5, 'numInputRows': 100, ...}

# Czekaj na zakończenie
query.awaitTermination()          # Blokuje na zawsze
query.awaitTermination(60)        # Blokuje max 60 sekund

# Zatrzymaj
query.stop()

# Wyjątek (jeśli był)
print(query.exception())
```

### 11.2 Wszystkie aktywne query

```python
# Lista aktywnych query
for q in spark.streams.active:
    print(f"Query: {q.name}, Status: {q.status}")

# Czekaj na dowolne zakończenie
spark.streams.awaitAnyTermination()
```

### 11.3 Explain - plan wykonania

```python
# Pokaż plan (przed start!)
df.explain(extended=True)

# Dla streaming:
df.writeStream.format("console").explain()
```

### 11.4 Metryki w lastProgress

```python
progress = query.lastProgress

print(f"Batch ID: {progress['batchId']}")
print(f"Input rows: {progress['numInputRows']}")
print(f"Input rows/sec: {progress['inputRowsPerSecond']}")
print(f"Processed rows/sec: {progress['processedRowsPerSecond']}")
print(f"Duration: {progress['durationMs']}")

# Szczegóły źródeł
for source in progress['sources']:
    print(f"Source: {source['description']}")
    print(f"Start offset: {source['startOffset']}")
    print(f"End offset: {source['endOffset']}")
```

### 11.5 Spark UI

Spark UI dostępny domyślnie na `http://localhost:4040` zawiera:
- **Structured Streaming** tab z metrykami
- **SQL** tab z planami query
- **Jobs/Stages** z detalami wykonania

---

## 12. Best Practices i wzorce

### 12.1 Struktura projektu

```
my-streaming-project/
├── src/
│   ├── __init__.py
│   ├── schemas.py          # Schematy danych
│   ├── transformations.py  # Logika transformacji
│   ├── sinks.py            # Funkcje sink
│   └── main.py             # Entry point
├── config/
│   ├── kafka.properties
│   └── spark.conf
├── checkpoints/            # Lokalne checkpointy (dev)
├── output/                 # Lokalne outputy (dev)
└── tests/
    └── test_transformations.py
```

### 12.2 Wzorzec: Separation of Concerns

```python
# schemas.py
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

PRICE_SCHEMA = StructType([
    StructField("currency", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),
])

# transformations.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp

def parse_kafka_value(df: DataFrame, schema: StructType) -> DataFrame:
    return (
        df
        .selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
    )

def add_event_time(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

# main.py
from schemas import PRICE_SCHEMA
from transformations import parse_kafka_value, add_event_time

def main():
    spark = create_spark_session()
    
    raw = read_from_kafka(spark)
    parsed = parse_kafka_value(raw, PRICE_SCHEMA)
    with_time = add_event_time(parsed)
    aggregated = compute_aggregations(with_time)
    
    query = write_to_sink(aggregated)
    query.awaitTermination()
```

### 12.3 Wzorzec: Graceful Shutdown

```python
import signal
import sys

query = None

def shutdown_handler(signum, frame):
    print("Shutting down gracefully...")
    if query:
        query.stop()
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ... start query ...
query = df.writeStream.format("console").start()
query.awaitTermination()
```

### 12.4 Wzorzec: Multiple Sinks

```python
# Jeden stream, wiele outputów
parsed_df = parse_kafka(raw_stream)

# Sink 1: Console (debug)
console_query = (
    parsed_df.writeStream
    .format("console")
    .queryName("debug-console")
    .start()
)

# Sink 2: Kafka (forward)
kafka_query = (
    parsed_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "processed")
    .option("checkpointLocation", "/checkpoint/kafka/")
    .queryName("kafka-forward")
    .start()
)

# Sink 3: Parquet (archiwum)
parquet_query = (
    parsed_df.writeStream
    .format("parquet")
    .option("path", "/archive/")
    .option("checkpointLocation", "/checkpoint/archive/")
    .queryName("archive")
    .start()
)

# Czekaj na wszystkie
spark.streams.awaitAnyTermination()
```

### 12.5 Wzorzec: Error Handling w foreachBatch

```python
def safe_write_to_db(batch_df, batch_id):
    try:
        batch_df.write.format("jdbc").mode("append").save()
        print(f"Batch {batch_id}: SUCCESS")
    except Exception as e:
        print(f"Batch {batch_id}: FAILED - {e}")
        # Opcjonalnie: zapisz do dead-letter queue
        batch_df.write.format("json").mode("append").save("/dlq/")

query = (
    df.writeStream
    .foreachBatch(safe_write_to_db)
    .option("checkpointLocation", "/checkpoint/db/")
    .start()
)
```

---

## 13. Kompletne przykłady end-to-end

### 13.1 Crypto Price Aggregation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, min, max, count
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# === KONFIGURACJA ===
SPARK_VERSION = "4.0.1"
SCALA_VERSION = "2.13"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"

PRICE_SCHEMA = StructType([
    StructField("currency", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp", StringType()),
])

# === SPARK SESSION ===
spark = (
    SparkSession.builder
    .appName("CryptoPriceAggregation")
    .master("local[*]")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# === READ FROM KAFKA ===
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crypto-prices")
    .option("startingOffsets", "latest")
    .load()
)

# === PARSE JSON ===
parsed = (
    raw_stream
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), PRICE_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
)

# === AGGREGATION: 1-minute windows ===
price_stats = (
    parsed
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("currency")
    )
    .agg(
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        count("*").alias("sample_count")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("currency"),
        col("avg_price"),
        col("min_price"),
        col("max_price"),
        col("sample_count")
    )
)

# === WRITE TO CONSOLE ===
query = (
    price_stats.writeStream
    .format("console")
    .option("truncate", "false")
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
```

### 13.2 Real-time Alert System

```python
from pyspark.sql.functions import col, lag, abs as spark_abs
from pyspark.sql.window import Window

# ... (setup jak wyżej) ...

# Wykryj skoki ceny > 5% w ciągu minuty
price_with_prev = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("currency")
    )
    .agg(
        first("price").alias("first_price"),
        last("price").alias("last_price")
    )
    .withColumn(
        "change_pct",
        ((col("last_price") - col("first_price")) / col("first_price") * 100)
    )
    .filter(spark_abs(col("change_pct")) > 5)  # Alert jeśli > 5%
)

# Wyślij alerty do Kafka
alerts_output = (
    price_with_prev
    .select(
        col("currency").alias("key"),
        to_json(struct("currency", "change_pct", "window")).alias("value")
    )
)

alert_query = (
    alerts_output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "price-alerts")
    .option("checkpointLocation", "/checkpoint/alerts/")
    .outputMode("update")
    .start()
)
```

### 13.3 Multi-source Join

```python
# Stream 1: Ceny
prices_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crypto-prices")
    .load()
    # ... parse ...
)

# Stream 2: Wolumeny
volumes_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crypto-volumes")
    .load()
    # ... parse ...
)

# Join z watermarkami
enriched = (
    prices_stream
    .withWatermark("price_time", "10 minutes")
    .join(
        volumes_stream.withWatermark("volume_time", "10 minutes"),
        expr("""
            prices.currency = volumes.currency AND
            prices.price_time >= volumes.volume_time - interval 5 minutes AND
            prices.price_time <= volumes.volume_time + interval 5 minutes
        """),
        "leftOuter"
    )
)
```

---

## 14. Troubleshooting - najczęstsze problemy

### 14.1 "AnalysisException: checkpointLocation must be specified"

**Problem:** Brak checkpointu dla sinka wymagającego stanu.

**Rozwiązanie:**
```python
.option("checkpointLocation", "/path/to/checkpoint/")
```

### 14.2 "Append output mode not supported"

**Problem:** Używasz `append` mode z agregacją bez watermarku.

**Rozwiązanie:**
```python
# Dodaj watermark
.withWatermark("event_time", "10 minutes")

# LUB zmień output mode
.outputMode("update")  # lub "complete"
```

### 14.3 "StreamingQueryException: ... multiple streaming aggregations"

**Problem:** Dwie agregacje z różnymi watermarkami.

**Rozwiązanie:** Spark wspiera tylko jedną agregację ze stanem. Rozdziel na dwa osobne query.

### 14.4 Dane nie pojawiają się (puste batche)

**Przyczyny i rozwiązania:**
1. **startingOffsets="latest"** + brak nowych danych → zmień na `earliest` do testów
2. **Watermark za agresywny** → zwiększ tolerancję
3. **Trigger za rzadki** → zmniejsz interwał
4. **Błąd parsowania** → sprawdź schemat

### 14.5 OutOfMemoryError w agregacjach

**Problem:** Za dużo stanu (brak watermarku lub za duży).

**Rozwiązanie:**
```python
# Dodaj/zmniejsz watermark
.withWatermark("event_time", "1 hour")  # zamiast "24 hours"

# Zwiększ pamięć
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

### 14.6 Checkpoint corruption po zmianie kodu

**Problem:** Zmieniono logikę, stary checkpoint niekompatybilny.

**Rozwiązanie:**
```bash
# Usuń stary checkpoint
rm -rf /checkpoint/old-query/

# Użyj nowego katalogu
.option("checkpointLocation", "/checkpoint/query-v2/")
```

### 14.7 "IllegalStateException: Cannot find column X"

**Problem:** Kolumna usunięta/zmieniona po checkpoincie.

**Rozwiązanie:** Usuń checkpoint i zacznij od nowa (lub zachowaj kompatybilność schematu).

---

## 📚 Dodatkowe zasoby

- [Oficjalna dokumentacja Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka + Spark Integration Guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)

---

*Ostatnia aktualizacja: Grudzień 2025*
