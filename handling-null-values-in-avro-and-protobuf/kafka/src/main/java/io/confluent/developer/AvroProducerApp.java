package io.confluent.developer;

import io.confluent.developer.avro.PurchaseAvro;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class AvroProducerApp {

        private static final Logger LOG = LoggerFactory.getLogger(AvroProducerApp.class);
        private final Random random = new Random();
        private final List<String> items = List.of("shoes", "sun-glasses", "t-shirt");

    private Producer<String, PurchaseAvro> producer;

    public AvroProducerApp(Producer<String, PurchaseAvro> producer) {
        this.producer = producer;
    }

        public List<PurchaseAvro> producePurchaseEvents() {
            PurchaseAvro.Builder purchaseBuilder = PurchaseAvro.newBuilder();



            List<PurchaseAvro> avroPurchaseEvents = new ArrayList<>();

                String avroTopic = "avro-purchase";

                PurchaseAvro avroPurchase = getPurchaseObjectAvro(purchaseBuilder);
                PurchaseAvro avroPurchaseII = getPurchaseObjectAvro(purchaseBuilder);

                avroPurchaseEvents.add(avroPurchase);
                avroPurchaseEvents.add(avroPurchaseII);

                avroPurchaseEvents.forEach(event -> producer.send(new ProducerRecord<>(avroTopic, event.getCustomerId(), event), ((metadata, exception) -> {
                    if (exception != null) {
                        System.err.printf("Producing %s resulted in error %s %n", event, exception);
                    } else {
                        System.out.printf("Produced record to topic with Avro schema at offset %s with timestamp %d %n", metadata.offset(), metadata.timestamp());
                    }
                })));



            return avroPurchaseEvents;
        }



        PurchaseAvro getPurchaseObjectAvro(PurchaseAvro.Builder purchaseAvroBuilder) {
            purchaseAvroBuilder.setCustomerId("Customer Null").setItem(null)
                    .setTotalCost(random.nextDouble() * random.nextInt(100));
            return purchaseAvroBuilder.build();
        }

       static Properties loadProperties() {
            try (InputStream inputStream = AvroProducerApp.class
                    .getClassLoader()
                    .getResourceAsStream("confluent.properties")) {
                Properties props = new Properties();
                props.load(inputStream);
                return props;
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }

        public static void main(String[] args) {
            Map<String, Object> avroProducerConfigs = new HashMap<>();

            Properties properties = loadProperties();

            properties.forEach((key, value) -> avroProducerConfigs.put((String) key, value));

            avroProducerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            avroProducerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            avroProducerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
            avroProducerConfigs.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, true);
            // Setting schema auto-registration to false since we already registered the schema manually following best practice
            avroProducerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);


            Producer<String, PurchaseAvro> avroProducer = new KafkaProducer<>(avroProducerConfigs);
            io.confluent.developer.AvroProducerApp producerApp = new io.confluent.developer.AvroProducerApp(avroProducer);
            producerApp.producePurchaseEvents();
        }
    }


