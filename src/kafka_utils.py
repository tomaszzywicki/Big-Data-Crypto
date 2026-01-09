import sys
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


def read_config(config_file: str) -> dict:
    conf = {}

    try:
        with open(config_file) as file:
            for line in file:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":  # there is something and it is not a comment
                    parameter, value = line.split("=", 1)
                    conf[parameter] = value.strip()
        return conf

    except FileNotFoundError:
        print(f"[!] Error: File not found: {config_file}")
        sys.exit(1)


def create_topic(conf: dict, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
    admin = AdminClient(conf)

    new_topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    fs = admin.create_topics(new_topics=[new_topic])

    for topic, f in fs.items():
        try:
            f.result()
            print(f"[+] Topic {topic} created")

        except Exception as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[!] Failed to create topic {topic}: {e}")
                sys.exit(1)
            else:
                print(f"[.] Topic {topic} already exists")
