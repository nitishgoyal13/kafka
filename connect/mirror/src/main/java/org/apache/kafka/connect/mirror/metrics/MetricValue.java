package org.apache.kafka.connect.mirror.metrics;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.connect.source.SourceRecord;

/***
 Created by nitish.goyal on 16/12/19
 ***/
public class MetricValue {

    private KafkaMetric kafkaMetric;
    private SourceRecord consumerRecord;

    public MetricValue(KafkaMetric kafkaMetric, SourceRecord consumerRecord) {
        this.kafkaMetric = kafkaMetric;
        this.consumerRecord = consumerRecord;
    }

    public KafkaMetric getKafkaMetric() {
        return kafkaMetric;
    }

    public void setKafkaMetric(KafkaMetric kafkaMetric) {
        this.kafkaMetric = kafkaMetric;
    }

    public SourceRecord getConsumerRecord() {
        return consumerRecord;
    }

    public void setConsumerRecord(SourceRecord consumerRecord) {
        this.consumerRecord = consumerRecord;
    }
}
