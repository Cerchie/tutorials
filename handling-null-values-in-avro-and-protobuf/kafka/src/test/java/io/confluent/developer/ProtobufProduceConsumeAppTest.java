package io.confluent.developer;


import io.confluent.developer.proto.Purchase;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class ProtobufProduceConsumeAppTest {
    private static final Map<String, Object> commonConfigs = new HashMap<>();
    private final Serializer<String> stringSerializer = new StringSerializer();
    private ProtoProducerApp protoProducerApp;
    private ProtoConsumerApp protoConsumerApp;

    @BeforeClass
    public static void beforeAllTests() throws IOException {
        commonConfigs.put("schema.registry.url", "mock://null-values-produce-consume-test");
        commonConfigs.put("proto.topic", "proto-records");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProduceProtoMultipleEvents() {
        KafkaProtobufSerializer protoSerializer
                = new KafkaProtobufSerializer();
        protoSerializer.configure(commonConfigs, false);
        MockProducer<String, Purchase> mockProtoProducer
                = new MockProducer<String, Purchase>(true, stringSerializer, (Serializer) protoSerializer);
        ProtoProducerApp protoProducerApp = new ProtoProducerApp(mockProtoProducer);
        List<Purchase> returnedProtoResults = protoProducerApp.producePurchaseEvents();

        List<KeyValue<String, Purchase>> expectedKeyValues =
                mockProtoProducer.history().stream().map(this::toKeyValue).collect(Collectors.toList());

        returnedProtoResults.forEach(c ->
        {
            String purchaseProtoId = c.getCustomerId();
            assertEquals("Customer Null", purchaseProtoId);
            assertEquals(returnedProtoResults.size(), 2);
        });

    }

    @Test
    public void testConsumeProtoEvents() {
        KafkaProtobufSerializer protoSerializer
                = new KafkaProtobufSerializer();
        protoSerializer.configure(commonConfigs, false);
        MockProducer<String, Purchase> mockProtoProducer
                = new MockProducer<String, Purchase>(true, stringSerializer, (Serializer) protoSerializer);
        ProtoProducerApp protoProducerApp = new ProtoProducerApp(mockProtoProducer);
        MockConsumer<String, Purchase> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        String topic = (String) commonConfigs.get("proto.topic");

        mockConsumer.schedulePollTask(() -> {
            addTopicPartitionsAssignment(topic, mockConsumer);
            addConsumerRecords(mockConsumer, protoProducerApp.producePurchaseEvents(), Purchase::getCustomerId, topic);
        });

        ProtoConsumerApp protoConsumerApp = new ProtoConsumerApp(mockConsumer);
        ConsumerRecords<String,Purchase> returnedProtoResults = protoConsumerApp.consumePurchaseEvents();
        List<Purchase> actualProtoResults = new ArrayList<>();

        returnedProtoResults.forEach(c ->
        {
            Purchase purchaseProto = c.value();
            assertEquals("Customer Null", purchaseProto.getCustomerId());
            actualProtoResults.add(purchaseProto);
        });
        assertEquals(2, actualProtoResults.size());

        mockConsumer.schedulePollTask(() -> protoConsumerApp.close());

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