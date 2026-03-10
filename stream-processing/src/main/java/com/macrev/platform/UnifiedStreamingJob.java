package com.macrev.platform;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.Duration;

public class UnifiedStreamingJob {

    private static final String KAFKA_BOOTSTRAP = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:9092"
    );

    private static final String TOPIC_USER_ACTIVITY =
            System.getenv().getOrDefault("KAFKA_TOPIC_USER_ACTIVITY", "events.user_activity.v1");

    private static final String TOPIC_ORDERS =
            System.getenv().getOrDefault("KAFKA_TOPIC_ORDERS", "events.orders.v1");

    private static final String TOPIC_PAYMENTS =
            System.getenv().getOrDefault("KAFKA_TOPIC_PAYMENTS", "events.payments.v1");

    private static final String TOPIC_DEVICE_TELEMETRY =
            System.getenv().getOrDefault("KAFKA_TOPIC_DEVICE_TELEMETRY", "events.device_telemetry.v1");


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(30000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP)
                .setTopics(
                        TOPIC_USER_ACTIVITY,
                        TOPIC_ORDERS,
                        TOPIC_PAYMENTS,
                        TOPIC_DEVICE_TELEMETRY
                )
                .setGroupId("uslp-stream-processor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents =
                env.fromSource(
                        source,
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "kafka-event-source"
                );

        rawEvents
                .name("raw-event-log")
                .map(event -> {
                    System.out.println("EVENT_RECEIVED: " + event);
                    return event;
                });

        env.execute("Unified Streaming Lakehouse Platform - Flink Job");
    }
}