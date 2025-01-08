# art-vinsguru-reactive-kafka

Tutorial on Kafka Event Driven Microservices With Java + Spring from Vinoth Selvaraj (Udemy)

---

## Section 3: Kafka Crash Course

#### 10. Topic command

Execute `bash` inside the container

```sh
docker exec -it kafka bash 
```

Create new topic `hello-world`:

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --create
```

Create new topic `order-events`:

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create
```

List topics:

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Describe topic:

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --describe
```

Delete topic:

```sh
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --delete
```

### 12. Documenting Commands

All the useful commands are present in [02-kafka-101](/01-workspace/02-kafka-101) folder


---

## Section 4: Reactor Kafka

---

## Section 5: Kafka Cluster

### 67. Cluster Demo

1. Start [docker-compose](/01-workspace/03-kafka-cluster/docker-compose.yaml)
2. Create topic with replicas:
    - `docker exec -it kafka1 bash`
    - `kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --partitions 2 --replication-factor 3`
3. Describe topic
    - `kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --describe`
    - Result:
        - ```
      Topic: order-events TopicId: BFZomcnRRyqKfgJDlw7gGQ PartitionCount: 2 ReplicationFactor: 3 Configs:
      Topic: order-events Partition: 0 Leader: 3 Replicas: 3,1,2 Isr: 3,1,2 Topic: order-events Partition: 1 Leader: 1
      Replicas: 1,2,3 Isr: 1,2,3
       ```
4. Stop one node:
    - stop leader of partition 0
    - `docker stop kafka3`
5. Describe changes:
    - `kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --describe`
    - Result:
        - ```
          Topic: order-events     TopicId: BFZomcnRRyqKfgJDlw7gGQ PartitionCount: 2       ReplicationFactor: 3    Configs:
          Topic: order-events     Partition: 0    Leader: 1       Replicas: 3,1,2 Isr: 1,2
          Topic: order-events     Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2     
          ```
        - Leader for partition 0 was changed
        - In-sync replicas (Isr) changed from `1,2,3` to `1,2`
6. Re-start broken node
    - `docker start kafka3`
7. Describe changes:
    - `kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --describe`
    - Result:
        - ```
          Topic: order-events     TopicId: BFZomcnRRyqKfgJDlw7gGQ PartitionCount: 2       ReplicationFactor: 3    Configs:
          Topic: order-events     Partition: 0    Leader: 1       Replicas: 3,1,2 Isr: 1,2,3
          Topic: order-events     Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
          ```
        - Leader for partition 0 **LEFT THE SAME**
        - In-sync replicas (Isr) changed to `1,2,3`

---

## Section 6: Best Practices

### 70. Producer acks

| acks | Behavior           |
|------|--------------------|
| -1   | all                |
| 0    | none (fire-forget) |
| 1    | leader             | 

### 71. min.insync.replica

- Default: `min.insync.replicas=1`
- If we have 3 nodes it is good to have at least 1 copy of data: `min.insync.replicas=2`
- `kafka-topics.sh --bootstrap-server localhost:9092 --topic order-events --create --replication-factor 3 --config min.insync.replica=2`

### 74. Idempotent Consumer

Recommended flow to prevent getting messages twice in case of ack failure (ex.: network issue)
1. Let the producer set unique message/event id (UUID)
2. Broker receives the messages
3. Check if they are present in the DB table
4. if yes - `duplicates` - simply ack and skip processing
5. if no - `new message` - process, insert into the db and then ack
