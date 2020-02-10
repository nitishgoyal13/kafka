package org.apache.kafka.common.metrics.riemann;

import com.aphyr.riemann.Proto;

import java.io.IOException;

/***
 Created by nitish.goyal on 05/11/19
 ***/
public interface RiemannEventPublisher {
    void publish(Proto.Event event) throws IOException;
}
