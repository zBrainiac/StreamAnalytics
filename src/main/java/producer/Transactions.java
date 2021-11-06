package producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Collections.unmodifiableList;

import java.util.Calendar;

/**
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath StreamAnalytics-0.0.2.0-SNAPSHOT.jar producer.Transactions kafka:9092
 *
 * @author Marcel Daeppen
 * @version 2021/11/03 08:21
 */

public class Transactions {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(Transactions.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";
    private static final List<String> transaction_currency_list = unmodifiableList(Arrays.asList(
            "USD", "EUR", "CHF"));

    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 200;

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
                publishMessage(producer);
                Thread.sleep(sleeptime);
            }
        }
    }

    private static Producer<String, byte[]> createProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURI);
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-Transactions");
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
        String key = String.valueOf(node.get("trx_id"));
        key = key.replace("\"", "");

        ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("transactions", key, valueJson);

        RecordMetadata msg = producer.send(eventrecord).get();

        LOG.info(String.format("Published %s/%d/%d : %s", msg.topic(), msg.partition(), msg.offset(), messageJsonObject));
    }

    // build random json object
    private static ObjectNode jsonObject() {

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
        String strDate = sdf.format(cal.getTime());

        ObjectNode report = objectMapper.createObjectNode();
        report.put("trx_id", "51" + (random.nextInt(89) + 10) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000) + "-" + (random.nextInt(8999) + 1000));
        report.put("currency_code", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("total", ThreadLocalRandom.current().nextDouble(6, 6656));
        report.put("transaction_time",strDate );

        return report;
    }

    public static void setsleeptime(long sleeptime) {
        Transactions.sleeptime = sleeptime;
    }
}