package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.*;

/**
 * Create Kafka Topic:
 *    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 9 --topic kafka_unbalanced
 *
 * output:
 *   Current time is 2020-08-30T15:45:24.485Z
 *
 * run:
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath StreamAnalytics-0.2.0.0.jar producer.FSICreditCartTRX localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2022/09/17 12:31
 */

public class KafkaProducerUnbalanced {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducerUnbalanced.class);
    private static final String LOGGERMSG = "Program prop set {}";

    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 1000;

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            brokerURI = args[0];
            String parm = "'use customized URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else if (args.length == 2) {
            brokerURI = args[0];
            setsleeptime(Long.parseLong(args[1]));
            String parm = "'use customized URI' = " + brokerURI + " & 'use customized sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        } else {
            String parm = "'use default URI' = " + brokerURI + " & 'use default sleeptime' = " + sleeptime;
            LOG.info(LOGGERMSG, parm);
        }

        //create kafka producer
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Kafka-Unbalanced");
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {

            WeightedRandomBag<Integer> itemDrops = new WeightedRandomBag<>();
            itemDrops.addEntry(0, 5.0);
            itemDrops.addEntry(1, 20.0);
            itemDrops.addEntry(2, 50.0);
            itemDrops.addEntry(3, 15.0);
            itemDrops.addEntry(4, 10.0);

            //prepare the record

            for (int i = 0; i < 1000000; i++) {
                Integer part = itemDrops.getRandom();
                Long timestamp = Instant.now().toEpochMilli();
                String key = UUID.randomUUID().toString();
                String recordValue = "msg_id=" + i + ", Current_time_is= " + Instant.now().toString();


                ProducerRecord<String, String> eventrecord = new ProducerRecord<>("kafka_unbalanced",
                        part,
                        timestamp,
                        key,
                        recordValue);

                //produce the record
                RecordMetadata msg = producer.send(eventrecord).get();

                LOG.info(new StringBuilder().append("Published ")
                        .append(msg.topic()).append("/")
                        .append(msg.partition()).append("/")
                        .append(msg.offset()).append(" : ")
                        .append(eventrecord)
                        .toString());

                producer.flush();

                // wait
                Thread.sleep(sleeptime);
            }
        }
    }

    private static class WeightedRandomBag<T> {

        private class Entry {
            double accumulatedWeight;
            T object;
        }

        private final List<Entry> entries = new ArrayList<>();
        private double accumulatedWeight;
        private final Random rand = new SecureRandom();

        public void addEntry(T object, double weight) {
            accumulatedWeight += weight;
            Entry e = new Entry();
            e.object = object;
            e.accumulatedWeight = accumulatedWeight;
            entries.add(e);
        }

        public T getRandom() {
            double r = rand.nextDouble() * accumulatedWeight;

            for (Entry entry: entries) {
                if (entry.accumulatedWeight >= r) {
                    return entry.object;
                }
            }
            return null; //should only happen when there are no entries
        }
    }

    public static void setsleeptime(long sleeptime) {
        KafkaProducerUnbalanced.sleeptime = sleeptime;
    }

}