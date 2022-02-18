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
import java.util.*;

import static java.util.Collections.unmodifiableList;


/**
 * run:
 *   cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 *   java -classpath StreamAnalytics-0.1.0.0.jar producer.CurrencyCode localhost:9092
 *
 * @author Marcel Daeppen
 * @version 2021/11/03 08:28
 */

public class CurrencyCode {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(CurrencyCode.class);
    private static final Random random = new SecureRandom();
    private static final String LOGGERMSG = "Program prop set {}";
    private static String brokerURI = "localhost:9092";
    private static long sleeptime = 1000;
    private static final List<String> transaction_currency_list = unmodifiableList(Arrays.asList(
            "USD", "EUR", "CHF"));

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
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder-CurrencyCode");
        config.put(ProducerConfig.ACKS_CONFIG,"1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");
        return new KafkaProducer<>(config);
    }

    private static void publishMessage(Producer<String, byte[]> producer) throws Exception {

        ObjectNode messageJsonObject = jsonObject();
        byte[] valueJson = objectMapper.writeValueAsBytes(messageJsonObject);

        final ObjectNode node = new ObjectMapper().readValue(valueJson, ObjectNode.class);
        String key = String.valueOf(node.get("currency_code"));
        key = key.replace("\"", "");


        ProducerRecord<String, byte[]> eventrecord = new ProducerRecord<>("currency_code", key, valueJson);

        RecordMetadata msg = producer.send(eventrecord).get();

        LOG.info(String.format("Published %s/%d/%d : %s", msg.topic(), msg.partition(), msg.offset(), messageJsonObject));
    }

    // build random json object
    private static ObjectNode jsonObject() {

        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
        String strDate = sdf.format(cal.getTime());

        ObjectNode report = objectMapper.createObjectNode();
        report.put("currency_code", transaction_currency_list.get(random.nextInt(transaction_currency_list.size())));
        report.put("eur_rate", (random.nextInt(20) + 90) / 100.0);
        report.put("rate_time",strDate );

        return report;
    }

    public static void setsleeptime(long sleeptime) {
        CurrencyCode.sleeptime = sleeptime;
    }
}