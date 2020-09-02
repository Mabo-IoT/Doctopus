import kafka


consumer = kafka.KafkaConsumer("custom_collector_json",bootstrap_servers="192.168.1.21:9092") 
for m in consumer:
    print(m)