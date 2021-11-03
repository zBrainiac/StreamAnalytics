
# start:
```
docker-compose up -d --scale flink-taskmanager=2
```

# stop & clean-up:
```
docker-compose down &&
docker rm -f $(docker ps -a -q) &&
docker volume rm $(docker volume ls -q)
```

# Add Host on macos x:
```
sudo -- sh -c -e "echo '127.0.0.1   kafka' >> /etc/hosts"
```

# Producer:
```
BOOTSTRAP_SERVERS_CONFIG = kafka:9092
```

# CLI conmands:
```
./kafka-topics.sh --bootstrap-server kafka:9092 --create --topic abc4711
./kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic abc4711
./bin/kafka-topics.sh --list --bootstrap-server kafka:9092
./bin/kafka-topics.sh --delete --bootstrap-server kafka:9092 --topic FXRate  &&
./bin/kafka-topics.sh --delete --bootstrap-server kafka:9092 --topic Transactions &&
./bin/kafka-topics.sh --delete --bootstrap-server kafka:9092 --topic currency_rates &&
./bin/kafka-topics.sh --list --bootstrap-server kafka:9092


./bin/kafka-topics.sh --delete --bootstrap-server kafka:9092 --topic

```
