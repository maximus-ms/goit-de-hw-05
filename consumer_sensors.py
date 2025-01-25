from kafka import KafkaProducer, KafkaConsumer
from configs import kafka_config
import json

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Зчитування повідомлень з початку
    enable_auto_commit=True,       # Автоматичне підтвердження зчитаних повідомлень
    group_id=f'{kafka_config["name"]}_group_1'   # Ідентифікатор групи споживачів
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Назва топіку
sensors_topic_name = f'{kafka_config['name']}_building_sensors'
temperature_alerts_topic_name = f'{kafka_config['name']}_temperature_alerts'
humidity_alerts_topic_name = f'{kafka_config['name']}_humidity_alerts'

# Підписка на тему
consumer.subscribe([sensors_topic_name])

print(f'Subscribed to topic "{sensors_topic_name}"')

# Обробка повідомлень з топіку
try:
    for message in consumer:
        print(f'RCV << {message.value}')
        if message.value['temperature'] > 40:
            message.value['message'] = 'High temperature alert!'
            producer.send(temperature_alerts_topic_name, message.value)
            producer.flush()
            print(f'SND >> {temperature_alerts_topic_name}: "{message.value}"')
        if message.value['humidity'] > 80:
            message.value['message'] = 'High humidity alert!'
        elif message.value['humidity'] < 20:
            message.value['message'] = 'Low humidity alert!'
        else:
            continue
        producer.send(humidity_alerts_topic_name, message.value)
        producer.flush()
        print(f'SND >> {humidity_alerts_topic_name}: "{message.value}"')

except Exception as e:
    print(f'An error occurred: {e}')
except KeyboardInterrupt as e:
    print(f'{e}. Exiting...')
finally:
    consumer.close()  # Закриття consumer
    producer.close()  # Закриття producer

