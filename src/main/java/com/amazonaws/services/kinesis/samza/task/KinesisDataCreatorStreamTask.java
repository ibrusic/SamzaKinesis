package com.amazonaws.services.kinesis.samza.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.nio.charset.Charset;

/**
 * Creates random data and pushes into a Kinesis stream.
 */
public class KinesisDataCreatorStreamTask implements StreamTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kinesis", "myFirstStream");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        String msg = new String((byte[])envelope.getMessage(), Charset.forName("UTF-8")).replace("Data", "Pata");
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, envelope.getKey(), msg.getBytes()));
    }
}
