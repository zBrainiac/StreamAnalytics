
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

# producer:
```
BOOTSTRAP_SERVERS_CONFIG = kafka:9092
```


## download release:
```
wget https://github.com/zBrainiac/StreamAnalytics/releases/download/StreamAnalytics_0.0.1/StreamAnalytics-0.0.1.0-SNAPSHOT.jar
```


## starting producers - FxRates

```
java -classpath StreamAnalytics-0.0.1.0-SNAPSHOT.jar producer.FxRates kafka:9092
```

## Console output
```
./bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic FXRate --property print.key=true --property key.separator=" - "

EUR - {"currency_code":"EUR","eur_rate":1.02,"rate_time":"2021-11-04 16:16:16.016"}
CHF - {"currency_code":"CHF","eur_rate":0.97,"rate_time":"2021-11-04 16:16:17.017"}
CHF - {"currency_code":"CHF","eur_rate":0.94,"rate_time":"2021-11-04 16:16:18.018"}
EUR - {"currency_code":"EUR","eur_rate":0.96,"rate_time":"2021-11-04 16:16:19.019"}
EUR - {"currency_code":"EUR","eur_rate":0.92,"rate_time":"2021-11-04 16:16:20.020"}
...

```

## starting producers - Transactions
```
java -classpath StreamAnalytics-0.0.1.0-SNAPSHOT.jar producer.Transactions kafka:9092

```

## Console output
```
./bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic Transactions --property print.key=true --property key.separator=" - "

5146-9824-9422-5845 - {"trx_id":"5146-9824-9422-5845","currency_code":"EUR","total":2100.2884258642616,"transaction_time":"2021-11-04 16:20:42.042"}
5171-3446-9888-4150 - {"trx_id":"5171-3446-9888-4150","currency_code":"EUR","total":2365.783633975278,"transaction_time":"2021-11-04 16:20:43.043"}
5183-1485-1442-5034 - {"trx_id":"5183-1485-1442-5034","currency_code":"EUR","total":4311.410868584851,"transaction_time":"2021-11-04 16:20:44.044"}
5191-4653-9906-4654 - {"trx_id":"5191-4653-9906-4654","currency_code":"CHF","total":1368.0098006066453,"transaction_time":"2021-11-04 16:20:45.045"}
5179-8505-1275-5461 - {"trx_id":"5179-8505-1275-5461","currency_code":"CHF","total":5159.681895688063,"transaction_time":"2021-11-04 16:20:46.046"}
...

```

# some Kafka CLI commands
```
./bin/kafka-topics.sh --list --bootstrap-server kafka:9092
./bin/kafka-topics.sh --delete --bootstrap-server kafka:9092 --topic FXRate  &&
./bin/kafka-topics.sh --delete --bootstrap-server kafka:9092 --topic Transactions &&

```
