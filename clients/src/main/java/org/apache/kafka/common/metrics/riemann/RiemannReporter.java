package org.apache.kafka.common.metrics.riemann;

import com.aphyr.riemann.Proto;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 Created by nitish.goyal on 05/11/19
 ***/
public class RiemannReporter implements MetricsReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RiemannReporter.class);
    private static final String PARTITION = "partition";
    private static final String TOPIC = "topic";
    private RiemannEventPublisher publisher;

    @Override
    public void init(List<KafkaMetric> metrics) {
        try {
            LOGGER.info("Initializing riemann reporter");
            if (null == publisher) {
                publisher = RiemannTcpClientPublisher.buildFromProperties();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while initialising Riemann reporter", e);
        }

    }

    @Override
    public void metricChange(KafkaMetric metric) {
        Proto.Event.Builder builder = buildEvent(metric);
        if (metric.metricValue() instanceof Double && (Double) metric.metricValue() > 0d) {
            MetricName metricName = metric.metricName();
            LOGGER.info("MetricName : {} Group: {} Value :  {} Topic : {} Partition : {} ",
                    metricName.name(), metricName.group(), metric.metricValue(), metricName.tags().get(TOPIC),
                    metricName.tags().get(PARTITION));
            Proto.Event event = builder.setMetricD((Double) metric.metricValue())
                    .addAttributes(buildMetricTypeAttribute("meter"))
                    .build();
            try {
                sendEvent(event);
            } catch (Exception e) {
                LOGGER.error("Error occurred while sending event to riemann : ", e);
            }
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        // Add support for metric removal
    }

    @Override
    public void close() {
        //Add support
    }

    @Override
    public void configure(Map<String, ?> configs) {
        //Add support
    }

    private Proto.Event.Builder buildEvent(KafkaMetric metric) {
        Proto.Event.Builder builder = Proto.Event.newBuilder();
        try {
            builder.setHost(InetAddress.getLocalHost().getCanonicalHostName());
            builder.setTime(System.currentTimeMillis() / 1000);
            builder.setService(String.format("kafka.ccr.%s.%s",
                    metric.metricName().group(),
                    metric.metricName().name()));
            builder.setDescription(metric.metricName().description());
            metric.metricName().tags().forEach((s, v) -> {
                //This is done to get metrics per partition
                if (s.equals(PARTITION) || s.equals(TOPIC)) {
                    builder.setState(v);
                } else {
                    LOGGER.debug("Tag Key : {}, Tag Value : {}", s, v);
                    builder.addTags(s);
                }
            });
        } catch (UnknownHostException e) {
            LOGGER.error("Couldn't determine current host", e);
        }
        return builder;
    }

    private Proto.Attribute buildMetricTypeAttribute(String type) {
        return Proto.Attribute.newBuilder().setKey("metricType").setValue(type).build();
    }

    private void sendEvent(Proto.Event event) throws IOException {
        if (publisher == null) {
            publisher = RiemannTcpClientPublisher.buildFromProperties();
        }
        publisher.publish(event);
    }

}
