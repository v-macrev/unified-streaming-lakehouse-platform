package com.macrev.platform.model;

import java.time.Instant;
import java.util.Objects;

public class EventEnvelope {

    private String eventId;
    private String eventType;
    private Instant eventTime;
    private Instant ingestionTime;
    private String producerVersion;
    private Integer schemaVersion;
    private String sourceSystem;
    private String partitionKey;
    private String traceId;

    public EventEnvelope() {
        // Required for Jackson deserialization.
    }

    public EventEnvelope(
            String eventId,
            String eventType,
            Instant eventTime,
            Instant ingestionTime,
            String producerVersion,
            Integer schemaVersion,
            String sourceSystem,
            String partitionKey,
            String traceId
    ) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.ingestionTime = ingestionTime;
        this.producerVersion = producerVersion;
        this.schemaVersion = schemaVersion;
        this.sourceSystem = sourceSystem;
        this.partitionKey = partitionKey;
        this.traceId = traceId;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public Instant getIngestionTime() {
        return ingestionTime;
    }

    public void setIngestionTime(Instant ingestionTime) {
        this.ingestionTime = ingestionTime;
    }

    public String getProducerVersion() {
        return producerVersion;
    }

    public void setProducerVersion(String producerVersion) {
        this.producerVersion = producerVersion;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getSourceSystem() {
        return sourceSystem;
    }

    public void setSourceSystem(String sourceSystem) {
        this.sourceSystem = sourceSystem;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public boolean isValid() {
        return eventId != null && !eventId.isBlank()
                && eventType != null && !eventType.isBlank()
                && eventTime != null
                && ingestionTime != null
                && producerVersion != null && !producerVersion.isBlank()
                && schemaVersion != null && schemaVersion > 0
                && sourceSystem != null && !sourceSystem.isBlank()
                && partitionKey != null && !partitionKey.isBlank()
                && traceId != null && !traceId.isBlank();
    }

    @Override
    public String toString() {
        return "EventEnvelope{" +
                "eventId='" + eventId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", ingestionTime=" + ingestionTime +
                ", producerVersion='" + producerVersion + '\'' +
                ", schemaVersion=" + schemaVersion +
                ", sourceSystem='" + sourceSystem + '\'' +
                ", partitionKey='" + partitionKey + '\'' +
                ", traceId='" + traceId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof EventEnvelope that)) {
            return false;
        }
        return Objects.equals(eventId, that.eventId)
                && Objects.equals(eventType, that.eventType)
                && Objects.equals(eventTime, that.eventTime)
                && Objects.equals(ingestionTime, that.ingestionTime)
                && Objects.equals(producerVersion, that.producerVersion)
                && Objects.equals(schemaVersion, that.schemaVersion)
                && Objects.equals(sourceSystem, that.sourceSystem)
                && Objects.equals(partitionKey, that.partitionKey)
                && Objects.equals(traceId, that.traceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                eventId,
                eventType,
                eventTime,
                ingestionTime,
                producerVersion,
                schemaVersion,
                sourceSystem,
                partitionKey,
                traceId
        );
    }
}