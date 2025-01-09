# stop the consumers before you enter this command

# dry-run
 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group cg \
    --topic hello-world \
    --reset-offsets \
    --shift-by -3 \
    --dry-run 

# reset offset by shifting the offset
 kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group cg \
    --topic hello-world \
    --reset-offsets \
    --shift-by -3 \
    --execute   

# reset by duration 
kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --topic hello-world \
    --group cg \
    --reset-offsets \
    --by-duration PT5M \
    --execute

# -- to the beginning
kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --topic hello-world \
    --group cg \
    --reset-offsets \
    --to-earliest \
    --execute

kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --topic order-events-dlt \
    --group con_con \
    --reset-offsets \
    --to-earliest \
    --execute

# -- to the end
kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --topic hello-world \
    --group cg \
    --reset-offsets \
    --to-latest \
    --execute   

# -- to date-time
kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --topic hello-world \
    --group cg \
    --reset-offsets \
    --to-datetime 2025-01-06T06:00:00.000 \
    --execute            