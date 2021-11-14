package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Create Kafka Topic:
 * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 5 --topic kafka_lookupid &&
 * bin/kafka-topics.sh --list --bootstrap-server localhost:9092 &&
 * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kafka_lookupid
 *
 * run:
 * cd /opt/cloudera/parcels/FLINK/lib/flink/examples/streaming &&
 * java -classpath StreamAnalytics-0.0.3.0.jar producer.RefDataGeoLocation
 *
 * @author Marcel Daeppen
 * @version 2021/08/07 14:28
 */

public class RefDataGeoLocation {

    private static final Logger LOG = LoggerFactory.getLogger(RefDataGeoLocation.class);
    private static final String LOGGERMSG = "Program prop set {}";

    private static String brokerURI = "kafka:9092";
    private static long sleeptime = 111;


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
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Feeder_RefData_GeoLocation");
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.hortonworks.smm.kafka.monitoring.interceptors.MonitoringProducerInterceptor");

        ArrayList<String> al = new ArrayList<>();
        al.add("{"+ "\"loc_id\"" + ":" + "0"+ "," + "\"city\"" + ":" + "\"Porrentruy\"" + "," + "\"lon\"" + ":" + "47.415327"+ "," + "\"lat\"" + ":" + "7.075221" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "1"+ "," + "\"city\"" + ":" + "\"Geneva\"" + "," + "\"lon\"" + ":" + "46.195602"+ "," + "\"lat\"" + ":" + "6.148113" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "2"+ "," + "\"city\"" + ":" + "\"Zürich\"" + "," + "\"lon\"" + ":" + "47.366667"+ "," + "\"lat\"" + ":" + "8.55" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "3"+ "," + "\"city\"" + ":" + "\"Basel\"" + "," + "\"lon\"" + ":" + "47.558395"+ "," + "\"lat\"" + ":" + "7.573271" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "4"+ "," + "\"city\"" + ":" + "\"Bern\"" + "," + "\"lon\"" + ":" + "46.916667"+ "," + "\"lat\"" + ":" + "7.466667" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "5"+ "," + "\"city\"" + ":" + "\"Lausanne\"" + "," + "\"lon\"" + ":" + "46.533333"+ "," + "\"lat\"" + ":" + "6.666667" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "6"+ "," + "\"city\"" + ":" + "\"Lucerne\"" + "," + "\"lon\"" + ":" + "47.083333"+ "," + "\"lat\"" + ":" + "8.266667" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "7"+ "," + "\"city\"" + ":" + "\"Lugano\"" + "," + "\"lon\"" + ":" + "46.009279"+ "," + "\"lat\"" + ":" + "8.955576" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "8"+ "," + "\"city\"" + ":" + "\"Sankt-Fiden\"" + "," + "\"lon\"" + ":" + "47.43162"+ "," + "\"lat\"" + ":" + "9.39845" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "9"+ "," + "\"city\"" + ":" + "\"Chur\"" + "," + "\"lon\"" + ":" + "46.856753"+ "," + "\"lat\"" + ":" + "9.526918" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "10"+ "," + "\"city\"" + ":" + "\"Schaffhausen\"" + "," + "\"lon\"" + ":" + "47.697316"+ "," + "\"lat\"" + ":" + "8.634929" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "11"+ "," + "\"city\"" + ":" + "\"Fribourg\"" + "," + "\"lon\"" + ":" + "46.79572"+ "," + "\"lat\"" + ":" + "7.154748" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "12"+ "," + "\"city\"" + ":" + "\"Neuchâtel\"" + "," + "\"lon\"" + ":" + "46.993089"+ "," + "\"lat\"" + ":" + "6.93005" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "13"+ "," + "\"city\"" + ":" + "\"Tripon\"" + "," + "\"lon\"" + ":" + "46.270839"+ "," + "\"lat\"" + ":" + "7.317785" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "14"+ "," + "\"city\"" + ":" + "\"Zug\"" + "," + "\"lon\"" + ":" + "47.172421"+ "," + "\"lat\"" + ":" + "8.517445" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "15"+ "," + "\"city\"" + ":" + "\"Frauenfeld\"" + "," + "\"lon\"" + ":" + "47.55993"+ "," + "\"lat\"" + ":" + "8.8998" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "16"+ "," + "\"city\"" + ":" + "\"Bellinzona\"" + "," + "\"lon\"" + ":" + "46.194902"+ "," + "\"lat\"" + ":" + "9.024729" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "17"+ "," + "\"city\"" + ":" + "\"Aarau\"" + "," + "\"lon\"" + ":" + "47.389616"+ "," + "\"lat\"" + ":" + "8.052354" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "18"+ "," + "\"city\"" + ":" + "\"Herisau\"" + "," + "\"lon\"" + ":" + "47.38271"+ "," + "\"lat\"" + ":" + "9.27186" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "19"+ "," + "\"city\"" + ":" + "\"Solothurn\"" + "," + "\"lon\"" + ":" + "47.206649"+ "," + "\"lat\"" + ":" + "7.516605" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "20"+ "," + "\"city\"" + ":" + "\"Schwyz\"" + "," + "\"lon\"" + ":" + "47.027858"+ "," + "\"lat\"" + ":" + "8.656112" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "21"+ "," + "\"city\"" + ":" + "\"Liestal\"" + "," + "\"lon\"" + ":" + "47.482779"+ "," + "\"lat\"" + ":" + "7.742975" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "22"+ "," + "\"city\"" + ":" + "\"Delémont\"" + "," + "\"lon\"" + ":" + "47.366429"+ "," + "\"lat\"" + ":" + "7.329005" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "23"+ "," + "\"city\"" + ":" + "\"Sarnen\"" + "," + "\"lon\"" + ":" + "46.898509"+ "," + "\"lat\"" + ":" + "8.250681" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "24"+ "," + "\"city\"" + ":" + "\"Altdorf\"" + "," + "\"lon\"" + ":" + "46.880422"+ "," + "\"lat\"" + ":" + "8.644409" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "25"+ "," + "\"city\"" + ":" + "\"Stansstad\"" + "," + "\"lon\"" + ":" + "46.97731"+ "," + "\"lat\"" + ":" + "8.34005" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "26"+ "," + "\"city\"" + ":" + "\"Glarus\"" + "," + "\"lon\"" + ":" + "47.04057"+ "," + "\"lat\"" + ":" + "9.068036" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "27"+ "," + "\"city\"" + ":" + "\"Appenzell\"" + "," + "\"lon\"" + ":" + "47.328414"+ "," + "\"lat\"" + ":" + "9.409647" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "28"+ "," + "\"city\"" + ":" + "\"Saignelégier\"" + "," + "\"lon\"" + ":" + "47.255435"+ "," + "\"lat\"" + ":" + "6.994608" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "29"+ "," + "\"city\"" + ":" + "\"Affoltern-am-Albis\"" + "," + "\"lon\"" + ":" + "47.281224"+ "," + "\"lat\"" + ":" + "8.45346" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "30"+ "," + "\"city\"" + ":" + "\"Cully\"" + "," + "\"lon\"" + ":" + "46.488301"+ "," + "\"lat\"" + ":" + "6.730109" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "31"+ "," + "\"city\"" + ":" + "\"Romont\"" + "," + "\"lon\"" + ":" + "46.696483"+ "," + "\"lat\"" + ":" + "6.918037" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "32"+ "," + "\"city\"" + ":" + "\"Aarberg\"" + "," + "\"lon\"" + ":" + "47.043835"+ "," + "\"lat\"" + ":" + "7.27357" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "33"+ "," + "\"city\"" + ":" + "\"Scuol\"" + "," + "\"lon\"" + ":" + "46.796756"+ "," + "\"lat\"" + ":" + "10.305946" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "34"+ "," + "\"city\"" + ":" + "\"Fleurier\"" + "," + "\"lon\"" + ":" + "46.903265"+ "," + "\"lat\"" + ":" + "6.582135" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "35"+ "," + "\"city\"" + ":" + "\"Unterkulm\"" + "," + "\"lon\"" + ":" + "47.30998"+ "," + "\"lat\"" + ":" + "8.11371" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "36"+ "," + "\"city\"" + ":" + "\"Stans\"" + "," + "\"lon\"" + ":" + "46.95805"+ "," + "\"lat\"" + ":" + "8.36609" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "37"+ "," + "\"city\"" + ":" + "\"Lichtensteig\"" + "," + "\"lon\"" + ":" + "47.337551"+ "," + "\"lat\"" + ":" + "9.084078" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "38"+ "," + "\"city\"" + ":" + "\"Yverdon-les-Bains\"" + "," + "\"lon\"" + ":" + "46.777908"+ "," + "\"lat\"" + ":" + "6.635502" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "39"+ "," + "\"city\"" + ":" + "\"Boudry\"" + "," + "\"lon\"" + ":" + "46.953019"+ "," + "\"lat\"" + ":" + "6.83897" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "40"+ "," + "\"city\"" + ":" + "\"Balsthal\"" + "," + "\"lon\"" + ":" + "47.31591"+ "," + "\"lat\"" + ":" + "7.693047" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "41"+ "," + "\"city\"" + ":" + "\"Dornach\"" + "," + "\"lon\"" + ":" + "47.478042"+ "," + "\"lat\"" + ":" + "7.616417" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "42"+ "," + "\"city\"" + ":" + "\"Lachen\"" + "," + "\"lon\"" + ":" + "47.19927"+ "," + "\"lat\"" + ":" + "8.85432" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "43"+ "," + "\"city\"" + ":" + "\"Payerne\"" + "," + "\"lon\"" + ":" + "46.82201"+ "," + "\"lat\"" + ":" + "6.93608" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "44"+ "," + "\"city\"" + ":" + "\"Baden\"" + "," + "\"lon\"" + ":" + "47.478029"+ "," + "\"lat\"" + ":" + "8.302764" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "45"+ "," + "\"city\"" + ":" + "\"BadZurzach\"" + "," + "\"lon\"" + ":" + "47.589169"+ "," + "\"lat\"" + ":" + "8.289621" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "46"+ "," + "\"city\"" + ":" + "\"Tafers\"" + "," + "\"lon\"" + ":" + "46.814829"+ "," + "\"lat\"" + ":" + "7.218519" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "47"+ "," + "\"city\"" + ":" + "\"Haslen\"" + "," + "\"lon\"" + ":" + "47.369308"+ "," + "\"lat\"" + ":" + "9.367519" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "48"+ "," + "\"city\"" + ":" + "\"Echallens\"" + "," + "\"lon\"" + ":" + "46.642498"+ "," + "\"lat\"" + ":" + "6.637324" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "49"+ "," + "\"city\"" + ":" + "\"Rapperswil-Jona\"" + "," + "\"lon\"" + ":" + "47.228942"+ "," + "\"lat\"" + ":" + "8.833889" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "50"+ "," + "\"city\"" + ":" + "\"Bulle\"" + "," + "\"lon\"" + ":" + "46.619499"+ "," + "\"lat\"" + ":" + "7.056743" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "51"+ "," + "\"city\"" + ":" + "\"Bülach\"" + "," + "\"lon\"" + ":" + "47.518898"+ "," + "\"lat\"" + ":" + "8.536967" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "52"+ "," + "\"city\"" + ":" + "\"Sankt_Gallen\"" + "," + "\"lon\"" + ":" + "47.43639"+ "," + "\"lat\"" + ":" + "9.388615" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "53"+ "," + "\"city\"" + ":" + "\"Wil\"" + "," + "\"lon\"" + ":" + "47.460507"+ "," + "\"lat\"" + ":" + "9.04389" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "54"+ "," + "\"city\"" + ":" + "\"Zofingen\"" + "," + "\"lon\"" + ":" + "47.289945"+ "," + "\"lat\"" + ":" + "7.947274" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "55"+ "," + "\"city\"" + ":" + "\"Vevey\"" + "," + "\"lon\"" + ":" + "46.465264"+ "," + "\"lat\"" + ":" + "6.841168" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "56"+ "," + "\"city\"" + ":" + "\"Renens\"" + "," + "\"lon\"" + ":" + "46.539894"+ "," + "\"lat\"" + ":" + "6.588096" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "57"+ "," + "\"city\"" + ":" + "\"Brugg\"" + "," + "\"lon\"" + ":" + "47.481527"+ "," + "\"lat\"" + ":" + "8.203014" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "58"+ "," + "\"city\"" + ":" + "\"Laufenburg\"" + "," + "\"lon\"" + ":" + "47.559248"+ "," + "\"lat\"" + ":" + "8.060446" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "59"+ "," + "\"city\"" + ":" + "\"La_Chaux-de-Fonds\"" + "," + "\"lon\"" + ":" + "47.104417"+ "," + "\"lat\"" + ":" + "6.828892" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "60"+ "," + "\"city\"" + ":" + "\"Andelfingen\"" + "," + "\"lon\"" + ":" + "47.594829"+ "," + "\"lat\"" + ":" + "8.679678" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "61"+ "," + "\"city\"" + ":" + "\"Dietikon\"" + "," + "\"lon\"" + ":" + "47.404446"+ "," + "\"lat\"" + ":" + "8.394984" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "62"+ "," + "\"city\"" + ":" + "\"Winterthur\"" + "," + "\"lon\"" + ":" + "47.50564"+ "," + "\"lat\"" + ":" + "8.72413" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "63"+ "," + "\"city\"" + ":" + "\"Thun\"" + "," + "\"lon\"" + ":" + "46.751176"+ "," + "\"lat\"" + ":" + "7.621663" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "64"+ "," + "\"city\"" + ":" + "\"LeLocle\"" + "," + "\"lon\"" + ":" + "47.059533"+ "," + "\"lat\"" + ":" + "6.752278" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "65"+ "," + "\"city\"" + ":" + "\"Bremgarten\"" + "," + "\"lon\"" + ":" + "47.352604"+ "," + "\"lat\"" + ":" + "8.329955" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "66"+ "," + "\"city\"" + ":" + "\"Tiefencastel\"" + "," + "\"lon\"" + ":" + "46.660138"+ "," + "\"lat\"" + ":" + "9.57883" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "67"+ "," + "\"city\"" + ":" + "\"Saint-Maurice\"" + "," + "\"lon\"" + ":" + "46.218257"+ "," + "\"lat\"" + ":" + "7.003196" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "68"+ "," + "\"city\"" + ":" + "\"Cernier\"" + "," + "\"lon\"" + ":" + "47.057356"+ "," + "\"lat\"" + ":" + "6.894757" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "69"+ "," + "\"city\"" + ":" + "\"Ostermundigen\"" + "," + "\"lon\"" + ":" + "46.956112"+ "," + "\"lat\"" + ":" + "7.487187" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "70"+ "," + "\"city\"" + ":" + "\"Estavayer-le-Lac\"" + "," + "\"lon\"" + ":" + "46.849125"+ "," + "\"lat\"" + ":" + "6.845805" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "71"+ "," + "\"city\"" + ":" + "\"Frutigen\"" + "," + "\"lon\"" + ":" + "46.58782"+ "," + "\"lat\"" + ":" + "7.64751" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "72"+ "," + "\"city\"" + ":" + "\"Muri\"" + "," + "\"lon\"" + ":" + "47.270428"+ "," + "\"lat\"" + ":" + "8.3382" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "73"+ "," + "\"city\"" + ":" + "\"Murten\"" + "," + "\"lon\"" + ":" + "46.92684"+ "," + "\"lat\"" + ":" + "7.110343" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "74"+ "," + "\"city\"" + ":" + "\"Rheinfelden\"" + "," + "\"lon\"" + ":" + "47.553587"+ "," + "\"lat\"" + ":" + "7.793839" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "75"+ "," + "\"city\"" + ":" + "\"Gersau\"" + "," + "\"lon\"" + ":" + "46.994189"+ "," + "\"lat\"" + ":" + "8.524996" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "76"+ "," + "\"city\"" + ":" + "\"Schüpfheim\"" + "," + "\"lon\"" + ":" + "46.951613"+ "," + "\"lat\"" + ":" + "8.017235" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "77"+ "," + "\"city\"" + ":" + "\"Saanen\"" + "," + "\"lon\"" + ":" + "46.489557"+ "," + "\"lat\"" + ":" + "7.259609" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "78"+ "," + "\"city\"" + ":" + "\"Olten\"" + "," + "\"lon\"" + ":" + "47.357058"+ "," + "\"lat\"" + ":" + "7.909101" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "79"+ "," + "\"city\"" + ":" + "\"Domat/Ems\"" + "," + "\"lon\"" + ":" + "46.834827"+ "," + "\"lat\"" + ":" + "9.450752" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "80"+ "," + "\"city\"" + ":" + "\"Münchwilen\"" + "," + "\"lon\"" + ":" + "47.47788"+ "," + "\"lat\"" + ":" + "8.99569" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "81"+ "," + "\"city\"" + ":" + "\"Horgen\"" + "," + "\"lon\"" + ":" + "47.255924"+ "," + "\"lat\"" + ":" + "8.598672" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "82"+ "," + "\"city\"" + ":" + "\"Willisau\"" + "," + "\"lon\"" + ":" + "47.119362"+ "," + "\"lat\"" + ":" + "7.991459" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "83"+ "," + "\"city\"" + ":" + "\"Rorschach\"" + "," + "\"lon\"" + ":" + "47.477166"+ "," + "\"lat\"" + ":" + "9.485434" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "84"+ "," + "\"city\"" + ":" + "\"Morges\"" + "," + "\"lon\"" + ":" + "46.511255"+ "," + "\"lat\"" + ":" + "6.495693" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "85"+ "," + "\"city\"" + ":" + "\"Interlaken\"" + "," + "\"lon\"" + ":" + "46.683872"+ "," + "\"lat\"" + ":" + "7.866376" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "86"+ "," + "\"city\"" + ":" + "\"Sursee\"" + "," + "\"lon\"" + ":" + "47.170881"+ "," + "\"lat\"" + ":" + "8.111132" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "87"+ "," + "\"city\"" + ":" + "\"Küssnacht\"" + "," + "\"lon\"" + ":" + "47.085571"+ "," + "\"lat\"" + ":" + "8.442057" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "88"+ "," + "\"city\"" + ":" + "\"Weinfelden\"" + "," + "\"lon\"" + ":" + "47.56571"+ "," + "\"lat\"" + ":" + "9.10701" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "89"+ "," + "\"city\"" + ":" + "\"Pfäffikon\"" + "," + "\"lon\"" + ":" + "47.365728"+ "," + "\"lat\"" + ":" + "8.78595" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "90"+ "," + "\"city\"" + ":" + "\"Meilen\"" + "," + "\"lon\"" + ":" + "47.270429"+ "," + "\"lat\"" + ":" + "8.643675" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "91"+ "," + "\"city\"" + ":" + "\"Langnau\"" + "," + "\"lon\"" + ":" + "46.93936"+ "," + "\"lat\"" + ":" + "7.78738" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "92"+ "," + "\"city\"" + ":" + "\"Kreuzlingen\"" + "," + "\"lon\"" + ":" + "47.650512"+ "," + "\"lat\"" + ":" + "9.175038" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "93"+ "," + "\"city\"" + ":" + "\"Nidau\"" + "," + "\"lon\"" + ":" + "47.129167"+ "," + "\"lat\"" + ":" + "7.238464" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "94"+ "," + "\"city\"" + ":" + "\"Igis\"" + "," + "\"lon\"" + ":" + "46.945308"+ "," + "\"lat\"" + ":" + "9.57218" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "95"+ "," + "\"city\"" + ":" + "\"Ilanz\"" + "," + "\"lon\"" + ":" + "46.773071"+ "," + "\"lat\"" + ":" + "9.204486" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "96"+ "," + "\"city\"" + ":" + "\"Einsiedeln\"" + "," + "\"lon\"" + ":" + "47.12802"+ "," + "\"lat\"" + ":" + "8.74319" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "97"+ "," + "\"city\"" + ":" + "\"Wangen\"" + "," + "\"lon\"" + ":" + "47.231995"+ "," + "\"lat\"" + ":" + "7.654479" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "98"+ "," + "\"city\"" + ":" + "\"Hinwil\"" + "," + "\"lon\"" + ":" + "47.29702"+ "," + "\"lat\"" + ":" + "8.84348" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "99"+ "," + "\"city\"" + ":" + "\"Hochdorf\"" + "," + "\"lon\"" + ":" + "47.168408"+ "," + "\"lat\"" + ":" + "8.291788" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "100"+ "," + "\"city\"" + ":" + "\"Thusis\"" + "," + "\"lon\"" + ":" + "46.697524"+ "," + "\"lat\"" + ":" + "9.440202" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "101"+ "," + "\"city\"" + ":" + "\"Lenzburg\"" + "," + "\"lon\"" + ":" + "47.384048"+ "," + "\"lat\"" + ":" + "8.181798" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "102"+ "," + "\"city\"" + ":" + "\"Dielsdorf\"" + "," + "\"lon\"" + ":" + "47.480247"+ "," + "\"lat\"" + ":" + "8.45628" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "103"+ "," + "\"city\"" + ":" + "\"Mörel-Filet\"" + "," + "\"lon\"" + ":" + "46.355548"+ "," + "\"lat\"" + ":" + "8.044112" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "104"+ "," + "\"city\"" + ":" + "\"Münster-Geschinen\"" + "," + "\"lon\"" + ":" + "46.491704"+ "," + "\"lat\"" + ":" + "8.272063" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "105"+ "," + "\"city\"" + ":" + "\"Martigny\"" + "," + "\"lon\"" + ":" + "46.101915"+ "," + "\"lat\"" + ":" + "7.073989" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "106"+ "," + "\"city\"" + ":" + "\"Brig-Glis\"" + "," + "\"lon\"" + ":" + "46.3145"+ "," + "\"lat\"" + ":" + "7.985796" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "107"+ "," + "\"city\"" + ":" + "\"Davos\"" + "," + "\"lon\"" + ":" + "46.797752"+ "," + "\"lat\"" + ":" + "9.82702" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "108"+ "," + "\"city\"" + ":" + "\"Uster\"" + "," + "\"lon\"" + ":" + "47.352097"+ "," + "\"lat\"" + ":" + "8.716687" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "109"+ "," + "\"city\"" + ":" + "\"Altstätten\"" + "," + "\"lon\"" + ":" + "47.376433"+ "," + "\"lat\"" + ":" + "9.554989" + "}");
        al.add("{"+ "\"loc_id\"" + ":" + "110"+ "," + "\"city\"" + ":" + "\"Courtelary\"" + "," + "\"lon\"" + ":" + "47.179369"+ "," + "\"lat\"" + ":" + "7.072954" + "}");


        try (Producer<Integer, String> producer = new KafkaProducer<>(properties)) {

            for (int il = 1; il <= 100000; ++il) {


                for (int i = 0; i < 111; i++) {
                    String recordValue = al.get(i);
                    Integer key = i ;

                    ProducerRecord<Integer, String> eventrecord = new ProducerRecord<>("refdata_geoLocation", key, recordValue);

                    //produce the eventrecord
                    RecordMetadata msg = producer.send(eventrecord).get();

                    LOG.info(String.format("Published %s/%d/%d : %s", msg.topic(), msg.partition(), msg.offset(), recordValue));
                    Thread.sleep(sleeptime);
                }

                producer.flush();
            }
        }
    }

    public static void setsleeptime(long sleeptime) {
        RefDataGeoLocation.sleeptime = sleeptime;
    }

}