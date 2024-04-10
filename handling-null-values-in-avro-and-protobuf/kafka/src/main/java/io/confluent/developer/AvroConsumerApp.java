package io.confluent.developer;

import io.confluent.developer.avro.PurchaseAvro;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

public class AvroConsumerApp implements AutoCloseable{

    private Consumer<String, PurchaseAvro> consumer;
    public AvroConsumerApp(Consumer<String, PurchaseAvro> consumer) {
        this.consumer = consumer;
    }

    public void close() {
        keepConsumingAvro = false;
        ExecutorService executorService = null;
        executorService.shutdown();
    }
    private volatile boolean keepConsumingAvro = true;
        public ConsumerRecords<String, PurchaseAvro> consumePurchaseEvents() {

            consumer.subscribe(Collections.singletonList("avro-purchase"));

            ConsumerRecords<String, PurchaseAvro> avroConsumerRecords = consumer.poll(Duration.ofSeconds(2));

            avroConsumerRecords.forEach(avroConsumerRecord -> {
                PurchaseAvro avroPurchase = avroConsumerRecord.value();
                System.out.print("Purchase details consumed from topic with Avro schema { ");
                System.out.printf("Customer: %s, ", avroPurchase.getCustomerId());
                System.out.printf("Total Cost: %f, ", avroPurchase.getTotalCost());
                System.out.printf("Item: %s } %n", avroPurchase.getItem());
            });
            return avroConsumerRecords;
        }
        static Properties loadProperties () {
            try (InputStream inputStream = AvroConsumerApp.class
                    .getClassLoader()
                    .getResourceAsStream("confluent.properties")) {
                Properties props = new Properties();
                props.load(inputStream);
                return props;
            } catch (IOException exception) {
                throw new RuntimeException(exception);
            }
        }
        public static void main (String[]args){

            Map<String, Object> avroConsumerConfigs = new HashMap<>();

            Properties properties = loadProperties();

            properties.forEach((key, value) -> avroConsumerConfigs.put((String) key, value));

            avroConsumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "null-value-consumer");
            avroConsumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            avroConsumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            avroConsumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            avroConsumerConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

            Consumer<String, PurchaseAvro> avroConsumer = new KafkaConsumer<>(avroConsumerConfigs);
            io.confluent.developer.AvroConsumerApp consumerApp = new io.confluent.developer.AvroConsumerApp(avroConsumer);
            consumerApp.consumePurchaseEvents();

        }

}