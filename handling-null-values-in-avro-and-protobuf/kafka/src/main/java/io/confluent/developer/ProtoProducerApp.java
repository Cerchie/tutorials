package io.confluent.developer;


import io.confluent.developer.proto.Purchase;
import io.confluent.developer.proto.Purchase.Builder;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ProtoProducerApp {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoProducerApp.class);
    private final Random random = new Random();

        private final List<String> items = List.of("shoes", "sun-glasses", "t-shirt");

        private Producer<String, Purchase> producer;

    private ProtoProducerApp protoProducerApp;
    private ProtoConsumerApp protoConsumerApp;


    public ProtoProducerApp(Producer<String, Purchase> producer) {
            this.producer = producer;
        }


    public List<Purchase> producePurchaseEvents() {

        Purchase.Builder purchaseBuilder = Purchase.newBuilder();

         List<Purchase> protoPurchaseEvents = new ArrayList<>();

            String protoTopic = "proto-purchase";

            Purchase protoPurchase = getPurchaseObjectProto(purchaseBuilder);
            Purchase protoPurchaseII = getPurchaseObjectProto(purchaseBuilder);

            protoPurchaseEvents.add(protoPurchase);
            protoPurchaseEvents.add(protoPurchaseII);

            protoPurchaseEvents.forEach(event -> producer.send(new ProducerRecord<>(protoTopic, event.getCustomerId(), event), ((metadata, exception) -> {
                if (exception != null) {
                    System.err.printf("Producing %s resulted in error %s %n", event, exception);
                } else {
                    System.out.printf("Produced record to topic with Protobuf schema at offset %s with timestamp %d %n", metadata.offset(), metadata.timestamp());
                }
            })));


        return protoPurchaseEvents;


    }





    Purchase getPurchaseObjectProto(Builder purchaseBuilder) {
        purchaseBuilder.clear();
        purchaseBuilder.setCustomerId("Customer Null")
                .setTotalCost(random.nextDouble() * random.nextInt(100));
        return purchaseBuilder.build();
    }

static Properties loadProperties() {
    try (InputStream inputStream = ProtoProducerApp.class
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
        Map<String, Object> protoProducerConfigs = new HashMap<>();
        Properties properties = loadProperties();
        properties.forEach((key, value) -> protoProducerConfigs.put((String) key, value));

        protoProducerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        protoProducerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaProtobufSerializer.class);
        // Setting schema auto-registration to false since we already registered the schema manually following best practice
        protoProducerConfigs.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);
        Producer<String, Purchase> protoProducer = new KafkaProducer<>(protoProducerConfigs);
        io.confluent.developer.ProtoProducerApp producerApp = new io.confluent.developer.ProtoProducerApp(protoProducer);
        producerApp.producePurchaseEvents();
    }}

