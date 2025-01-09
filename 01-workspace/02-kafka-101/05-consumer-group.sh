
# create console producer
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic hello-world \
    --property key.separator=: \
    --property parse.key=true

# create console consumer with a group
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic hello-world \
    --property print.offset=true \
    --property print.key=true \
    --group name

# list all the consumer groups
 kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list 

 # describe a consumer group
 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group cg \
    --describe  
    
 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group con_con \
    --describe

kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic order-events-dlt \
    --property print.offset=true \
    --property print.key=true \
    --group con_con \
    --from-beginning