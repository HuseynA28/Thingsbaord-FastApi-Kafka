docker exec -it  kafka-topics --create --topic my_topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092


docker exec -it main-kafka-main-kafka-1 kafka-topics --create --topic my_topic --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092