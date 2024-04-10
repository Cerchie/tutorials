package io.confluent.developer;


import io.confluent.developer.avro.PurchaseAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class AvroProduceConsumeAppTest {
    private static final Map<String, Object> commonConfigs = new HashMap<>();
    private final Serializer<String> stringSerializer = new StringSerializer();
    private AvroProducerApp avroProducerApp;
    private AvroConsumerApp avroConsumerApp;

    @BeforeClass
    public static void beforeAllTests() throws IOException {
            commonConfigs.put("schema.registry.url", "mock://null-values-produce-consume-test");
            commonConfigs.put("avro.topic", "avro-records");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProduceAvroMultipleEvents() {

        KafkaAvroSerializer avroSerializer
                = new KafkaAvroSerializer();
        avroSerializer.configure(commonConfigs, false);
        MockProducer<String, PurchaseAvro> mockAvroProducer
                = new MockProducer<String, PurchaseAvro>(true, stringSerializer, (Serializer) avroSerializer);
        AvroProducerApp avroProducerApp = new AvroProducerApp(mockAvroProducer);
        List<PurchaseAvro> returnedAvroResults = avroProducerApp.producePurchaseEvents();

        returnedAvroResults.forEach(c ->
        {
            String purchaseAvroId = c.getCustomerId();
             String purchaseItem = c.getItem();
            assertEquals("Customer Null", purchaseAvroId);
            assertEquals(null, purchaseItem);
            assertEquals(returnedAvroResults.size(), 2);
        });

    }

    @Test
    public void testConsumeAvroEvents() {
        KafkaAvroSerializer avroSerializer
                = new KafkaAvroSerializer();
        avroSerializer.configure(commonConfigs, false);
        MockProducer<String, PurchaseAvro> mockAvroProducer
                = new MockProducer<String, PurchaseAvro>(true, stringSerializer, (Serializer) avroSerializer);
        AvroProducerApp avroProducerApp = new AvroProducerApp(mockAvroProducer);
        MockConsumer<String, PurchaseAvro> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        String topic = (String) commonConfigs.get("avro.topic");

        mockConsumer.schedulePollTask(() -> {
            addTopicPartitionsAssignment(topic, mockConsumer);
            addConsumerRecords(mockConsumer, avroProducerApp.producePurchaseEvents(), PurchaseAvro::getCustomerId, topic);
        });
        AvroConsumerApp avroConsumerApp = new AvroConsumerApp(mockConsumer);
        ConsumerRecords<String,PurchaseAvro> returnedAvroResults = avroConsumerApp.consumePurchaseEvents();
        List<PurchaseAvro> actualAvroResults = new ArrayList<>();

        returnedAvroResults.forEach(c ->
        {
            PurchaseAvro purchaseAvro = c.value();
            assertEquals("Customer Null", purchaseAvro.getCustomerId());
            assertEquals(null,purchaseAvro.getItem());
            actualAvroResults.add(purchaseAvro);
        });
        assertEquals(2, actualAvroResults.size());

        mockConsumer.schedulePollTask(() -> avroConsumerApp.close());
    }

    private <K, V> KeyValue<K, V> toKeyValue(final ProducerRecord<K, V> producerRecord) {
        return KeyValue.pair(producerRecord.key(), producerRecord.value());
    }

    private <V> void addTopicPartitionsAssignment(final String topic,
                                                  final MockConsumer<String, V> mockConsumer) {
        final TopicPartition topicPartition = new TopicPartition(topic, 0);
        final Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        mockConsumer.rebalance(Collections.singletonList(topicPartition));
        mockConsumer.updateBeginningOffsets(beginningOffsets);
    }

    private <V> void addConsumerRecords(final MockConsumer<String, V> mockConsumer,
                                        final List<V> records,
                                        final Function<V, String> keyFunction,
                                        final String topic) {
        AtomicInteger offset = new AtomicInteger(0);
        records.stream()
                .map(r -> new ConsumerRecord<>(topic, 0, offset.getAndIncrement(), keyFunction.apply(r), r))
                .forEach(mockConsumer::addRecord);
    }
}