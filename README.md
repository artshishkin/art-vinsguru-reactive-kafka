# art-vinsguru-reactive-kafka

Tutorial on  Kafka Event Driven Microservices With Java + Spring from Vinoth Selvaraj (Udemy)

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

