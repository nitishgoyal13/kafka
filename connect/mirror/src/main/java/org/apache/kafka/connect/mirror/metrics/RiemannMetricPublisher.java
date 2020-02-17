package org.apache.kafka.connect.mirror.metrics;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.riemann.RiemannReporter;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by mudit.g on Dec, 2019
 ***/
public class RiemannMetricPublisher implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RiemannMetricPublisher.class);

    private static final Map<TopicPartition, MetricValue> REPLICATION_LATENCY_METRIC_MAP = new ConcurrentHashMap<>();
    private static final Map<TopicPartition, MetricValue> REPLICATION_LATENCY_PER_HOST_METRIC_MAP = new ConcurrentHashMap<>();
    private static final Map<TopicPartition, MetricValue> OFFSET_METRIC_MAP = new ConcurrentHashMap<>();
    private static final Map<TopicPartition, KafkaMetric> BYTE_RATE_METRIC_MAP = new ConcurrentHashMap<>();
    private static final String LATENCY_METRIC = "replication-latency-ms";
    private static final String LATENCY_METRIC_DESCRIPTION = "Time it takes for records to replicate from source to target cluster";

    private static final Measurable DEFAULT_VALUE_PROVIDER = (MetricConfig config, long now) -> (double) 0;
    private static final String PARTITION = "partition";
    private static final String TOPIC = "topic";
    private static final String SOURCE_OFFSET = "offset";

    private MetricValue replicationLatency = null;

    private final Metrics metrics;
    private final ScheduledExecutorService executor;
    private JSONParser jsonParser;

    public RiemannMetricPublisher() {
        this.metrics = new Metrics();
        jsonParser = new JSONParser();
        this.executor = Executors.newScheduledThreadPool(5);
    }

    public void addReplicationLatencyMetric(TopicPartition topicPartition, SourceRecord record) {

        if (REPLICATION_LATENCY_METRIC_MAP.containsKey(topicPartition)) {
            REPLICATION_LATENCY_METRIC_MAP.get(topicPartition).setConsumerRecord(record);
        } else {
            Map<String, String> tags = new HashMap<>();
            tags.put(PARTITION, Integer.toString(topicPartition.partition()));
            MetricName metricName = metrics.metricName(
                    LATENCY_METRIC,
                    String.format("%s", topicPartition.topic()),
                    LATENCY_METRIC_DESCRIPTION,
                    tags);
            KafkaMetric kafkaMetric = new KafkaMetric(new Object(),
                    Objects.requireNonNull(metricName),
                    DEFAULT_VALUE_PROVIDER,
                    null,
                    Time.SYSTEM);
            REPLICATION_LATENCY_METRIC_MAP.put(topicPartition, new MetricValue(kafkaMetric, record));
        }
    }

    public void addReplicationLatencyPerHostMetric(TopicPartition topicPartition, SourceRecord record) {

        if (REPLICATION_LATENCY_PER_HOST_METRIC_MAP.containsKey(topicPartition)) {
            REPLICATION_LATENCY_PER_HOST_METRIC_MAP.get(topicPartition).setConsumerRecord(record);
        } else {
            Map<String, String> tags = new HashMap<>();
            tags.put(PARTITION, Integer.toString(topicPartition.partition()));
            try {
                MetricName metricName = metrics.metricName(
                        LATENCY_METRIC,
                        String.format("host." + "%s" + ".topic." + "%s",
                                InetAddress.getLocalHost().getCanonicalHostName(),
                                topicPartition.topic()),
                        LATENCY_METRIC_DESCRIPTION,
                        tags);
                KafkaMetric kafkaMetric = new KafkaMetric(new Object(),
                        Objects.requireNonNull(metricName),
                        DEFAULT_VALUE_PROVIDER,
                        null,
                        Time.SYSTEM);
                REPLICATION_LATENCY_PER_HOST_METRIC_MAP.put(topicPartition, new MetricValue(kafkaMetric, record));
            } catch (UnknownHostException e) {
                LOGGER.error("Error occurred while getting localhost : ", e);
            }
        }
    }

    public void addLatency(TopicPartition topicPartition, SourceRecord record) {
        Map<String, String> tags = new HashMap<>();
        tags.put(TOPIC, topicPartition.topic());
        MetricName metricName = metrics.metricName(
                LATENCY_METRIC,
                "latency",
                LATENCY_METRIC_DESCRIPTION,
                tags);
        KafkaMetric kafkaMetric = new KafkaMetric(new Object(),
                Objects.requireNonNull(metricName),
                DEFAULT_VALUE_PROVIDER,
                null,
                Time.SYSTEM);
        if (replicationLatency == null) {
            replicationLatency = new MetricValue(kafkaMetric, record);
        } else {
            replicationLatency.setConsumerRecord(record);
            replicationLatency.setKafkaMetric(kafkaMetric);
        }
    }

    public void addRecordBytesMetric(TopicPartition topicPartition, Measurable metricValue) {
        if (BYTE_RATE_METRIC_MAP.containsKey(topicPartition)) {
            BYTE_RATE_METRIC_MAP.get(topicPartition).setMetricValueProvider(metricValue);
        } else {
            Map<String, String> tags = new HashMap<>();
            tags.put(PARTITION, Integer.toString(topicPartition.partition()));
            MetricName metricName = metrics.metricName(
                    "byte-rate",
                    String.format("%s", topicPartition.topic()),
                    "Average number of bytes replicated per second.",
                    tags);
            KafkaMetric kafkaMetric = new KafkaMetric(new Object(),
                    Objects.requireNonNull(metricName),
                    Objects.requireNonNull(metricValue),
                    null,
                    Time.SYSTEM);
            BYTE_RATE_METRIC_MAP.put(topicPartition, kafkaMetric);
        }
    }

    public void addOffsetMetric(TopicPartition topicPartition, SourceRecord record) {

        if (OFFSET_METRIC_MAP.containsKey(topicPartition)) {
            OFFSET_METRIC_MAP.get(topicPartition).setConsumerRecord(record);
        } else {
            try {
                Map<String, String> tags = new HashMap<>();
                tags.put(PARTITION, Integer.toString(topicPartition.partition()));
                MetricName metricName = metrics.metricName(
                        SOURCE_OFFSET,
                        String.format("topic." + "%s", topicPartition.topic()),
                        "Offset of the source cluster",
                        tags);
                KafkaMetric kafkaMetric = new KafkaMetric(new Object(),
                        Objects.requireNonNull(metricName),
                        DEFAULT_VALUE_PROVIDER,
                        null,
                        Time.SYSTEM);
                OFFSET_METRIC_MAP.put(topicPartition, new MetricValue(kafkaMetric, record));
            } catch (Exception e) {
                LOGGER.error("Error while capturing source offset");
            }
        }
    }

    public void startPublishingAllTopicMetrics() {
        RiemannReporter riemannReporter = new RiemannReporter();
        riemannReporter.init(Lists.newArrayList());

        Runnable task = () -> {
            try {
                if (replicationLatency != null) {
                    try {
                        Measurable metric = getTimestampDiff(replicationLatency);
                        replicationLatency.getKafkaMetric().setMetricValueProvider(metric);
                        riemannReporter.metricChange(replicationLatency.getKafkaMetric());
                    } catch (Exception e) {
                        LOGGER.error("Error running replication latency executor for all topics", e);
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Error running task executor", e);
            }
        };
        executor.scheduleAtFixedRate(task, 10000L, 1000L, TimeUnit.MILLISECONDS);
    }

    public void startPublishing() {
        RiemannReporter riemannReporter = new RiemannReporter();
        riemannReporter.init(Lists.newArrayList());

        Runnable task = () -> {
            try {
                LOGGER.debug("Executing runnable task");
                REPLICATION_LATENCY_METRIC_MAP.forEach((topicPartition, metricValue) -> {
                    try {
                        Measurable metric = getTimestampDiff(metricValue);
                        metricValue.getKafkaMetric().setMetricValueProvider(metric);
                        riemannReporter.metricChange(metricValue.getKafkaMetric());
                    } catch (Exception e) {
                        LOGGER.error("Error running replication latency executor", e);
                    }
                });
                /*REPLICATION_LATENCY_PER_HOST_METRIC_MAP.forEach((topicPartition, metricValue) -> {
                    try {
                        Measurable metric = getTimestampDiff(metricValue);
                        metricValue.getKafkaMetric().setMetricValueProvider(metric);
                        riemannReporter.metricChange(metricValue.getKafkaMetric());
                    } catch (Exception e) {
                        LOGGER.error("Error running replication latency executor per host", e);
                    }
                });*/
                /*BYTE_RATE_METRIC_MAP.forEach((topicPartition, kafkaMetric) -> {
                    try {
                        riemannReporter.metricChange(kafkaMetric);
                    } catch (Exception e) {
                        LOGGER.error("Error running byte rate executor", e);
                    }
                });*/
                OFFSET_METRIC_MAP.forEach((topicPartition, metricValue) -> {
                    try {
                        Measurable metric = getOffset(metricValue);
                        metricValue.getKafkaMetric().setMetricValueProvider(metric);
                        riemannReporter.metricChange(metricValue.getKafkaMetric());
                    } catch (Exception e) {
                        LOGGER.error("Error running offset executor", e);
                    }
                });
                clearMaps();
            } catch (Exception e) {
                clearMaps();
                LOGGER.error("Error running task executor", e);
            }
        };
        executor.scheduleAtFixedRate(task, 60L, 60L, TimeUnit.SECONDS);
    }

    private void clearMaps() {
        REPLICATION_LATENCY_METRIC_MAP.clear();
        REPLICATION_LATENCY_PER_HOST_METRIC_MAP.clear();
        OFFSET_METRIC_MAP.clear();
        BYTE_RATE_METRIC_MAP.clear();
    }


    private Measurable getTimestampDiff(MetricValue metricValue) {
        SourceRecord record = metricValue.getConsumerRecord();
        long currentTime = System.currentTimeMillis();
        Measurable value = DEFAULT_VALUE_PROVIDER;
        //TODO Need to enhance it for other clients
        try {
            if (record.value() instanceof byte[]) {
                //All clients which use events ingestion client, populates ingestionTime
                JSONObject json = ((JSONObject) jsonParser.parse(new String((byte[]) record.value())));
                Object time = json.get("ingestionTime") == null
                        ? json.get("time")
                        : json.get("ingestionTime");
                if (time instanceof Long) {
                    long timeStamp = (Long) time;
                    value = (config, now) -> currentTime - timeStamp;
                }
            } else {
                value = getRecordTimestampDiffValue(record);
            }
        } catch (ParseException e) {
            //Do nothing. Logging this exception is creating lot of noise in the logs
        } catch (Exception e) {
            LOGGER.error("Error occurred while getting the latency :", e);
        }
        return value;
    }

    private Measurable getOffset(MetricValue metricValue) {
        Measurable value = DEFAULT_VALUE_PROVIDER;
        try {
            SourceRecord record = metricValue.getConsumerRecord();
            value = (config, now) -> {
                Long offset = (Long) record.sourceOffset().get(SOURCE_OFFSET);
                return offset.doubleValue();
            };
        } catch (Exception e) {
            LOGGER.error("Error while calculating source offset ", e);
        }
        return value;
    }

    private Measurable getRecordTimestampDiffValue(SourceRecord record) {
        long currentTime = System.currentTimeMillis();
        return (config, now) -> currentTime - record.timestamp();
    }

    @Override
    public void close() {
        try {
            LOGGER.info("attempt to shutdown executor");
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.info("tasks interrupted");
        } finally {
            if (!executor.isTerminated()) {
                LOGGER.info("cancel non-finished tasks");
            }
            executor.shutdownNow();
            LOGGER.info("shutdown finished");
        }
    }
}
