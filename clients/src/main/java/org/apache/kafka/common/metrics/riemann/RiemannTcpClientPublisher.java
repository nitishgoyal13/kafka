package org.apache.kafka.common.metrics.riemann;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.RiemannClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RiemannTcpClientPublisher implements RiemannEventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(RiemannTcpClientPublisher.class);

    private final RiemannClient riemann;

    public RiemannTcpClientPublisher(RiemannClient riemann) {
        this.riemann = riemann;
    }

    @Override
    public void publish(Proto.Event event) throws IOException {
        riemann.sendEventsWithAck(event);
    }

    private void connect() throws IOException {
        this.riemann.connect();
    }

    public static RiemannTcpClientPublisher buildFromProperties() throws IOException {
        //TODO Get it from config and get separate riemann for it
        //String host = props.getString("kafka.riemann.metrics.reporter.publisher.host", "127.0.0.1");
        //Integer port = props.getInt("kafka.riemann.metrics.reporter.publisher.port", 5555);
        //String host = "stg-riemanngg001.phonepe.nm2";
        String host = "prd-riemannkafka001.phonepe.nm5";
        Integer port = 5555;

        RiemannTcpClientPublisher publisher = new RiemannTcpClientPublisher(RiemannClient.tcp(host, port));
        LOGGER.info("Riemann Host : {}", host);
        publisher.connect();

        LOGGER.info("Connection established to the host");
        return publisher;
    }
}
