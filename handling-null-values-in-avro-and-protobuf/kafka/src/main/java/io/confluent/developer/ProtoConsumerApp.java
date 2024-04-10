package io.confluent.developer;

import io.confluent.developer.proto.Purchase;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
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

public class ProtoConsumerApp implements AutoCloseable {

    private Consumer<String, Purchase> consumer;
    public ProtoConsumerApp(Consumer<String, Purchase> consumer) {
        this.consumer = consumer;
    }

    public void close() {
        ExecutorService executorService = null;
        executorService.shutdown();
    }
    private volatile boolean keepConsumingProto = true;
    public ConsumerRecords<String, Purchase>  consumePurchaseEvents()  {


         consumer.subscribe(Collections.singletonList("proto-purchase"));

         ConsumerRecords<String, Purchase> protoConsumerRecords = consumer.poll(Duration.ofSeconds(2));
         protoConsumerRecords.forEach(protoConsumerRecord -> {
             Purchase protoPurchase = protoConsumerRecord.value();
             System.out.print("Purchase details consumed from topic with Protobuf schema { ");
             System.out.printf("Customer: %s, ", protoPurchase.getCustomerId());
             System.out.printf("Total Cost: %f, ", protoPurchase.getTotalCost());
             System.out.printf("Item: %s } %n", protoPurchase.getItem());
         });
            return protoConsumerRecords;
     }


         static Properties loadProperties () {
             try (InputStream inputStream = ProtoConsumerApp.class
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
             Properties properties = loadProperties();
             Map<String, Object> protoConsumerConfigs = new HashMap<>();

             properties.forEach((key, value) -> protoConsumerConfigs.put((String) key, value));
             protoConsumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-course-consumer");
             protoConsumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

             protoConsumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
             protoConsumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
             protoConsumerConfigs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Purchase.class);
             Consumer<String, Purchase> protoConsumer = new KafkaConsumer<>(protoConsumerConfigs);
             io.confluent.developer.ProtoConsumerApp consumerApp = new io.confluent.developer.ProtoConsumerApp(protoConsumer);
             consumerApp.consumePurchaseEvents();
         }
     }
