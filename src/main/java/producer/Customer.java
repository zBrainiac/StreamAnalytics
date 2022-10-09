package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Properties;

import java.security.SecureRandom;
import java.util.Random;

/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath StreamAnalytics-0.2.0.0.jar producer.Customer localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2022/10/09 08:21
 */

public class Customer {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Customer.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";
    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 5000;

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

        try (Producer<String, byte[]> producer = createProducer()) {
            for (int i = 0; i < 1000000; i++) {
                for (int ii = 0; ii < 100; ii++) {
                    publishMessage(producer);
                }
                LOG.info(String.format("loop: %d", i));
                Thread.sleep(sleeptime);
            }
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Customer");
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {
        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        final ObjectNode node = new ObjectMapper().readValue(valueJson, ObjectNode.class);
        String key = String.valueOf(node.get("customer_id"));
        key = key.replace("\"", "");

        ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("customer", key, valueJson);

        RecordMetadata msg = producer.send(eventrecord).get();

        LOG.info(String.format("Published %s/%d/%d : %s", msg.topic(), msg.partition(), msg.offset(), messageJsonObject));
    }

    // build random json object
    private static ObjectNode jsonObject() {

        Faker faker = new Faker(new Locale("de-CH"));

        ObjectNode report = objectMapper.createObjectNode();
        report.put("customer_id", (random.nextInt(101)));
        report.put("firstName", faker.name().firstName());
        report.put("lastName", faker.name().lastName());
        report.put("address", faker.address().streetAddress());
        report.put("city", faker.address().city());
        report.put("IdNumber", faker.idNumber().valid());
        report.put("Nation", faker.address().country());
        report.put("PhoneNumber", faker.phoneNumber().cellPhone());
        report.put("update_ts", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString());

        return report;
    }

    public static void setsleeptime(long sleeptime) {
        Customer.sleeptime = sleeptime;
    }
}