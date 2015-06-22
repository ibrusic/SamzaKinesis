package com.amazonaws.services.kinesis.samza.task;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.HashMap;
import java.util.Map;

/**
 * Creates random data and pushes into a Kinesis stream.
 */
public class KinesisDataCreatorStreamTask implements StreamTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kinesis", "myFirstStream");
    private static final int NUM_SHARDS = 5;
    private static int cnt = 0;

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator taskCoordinator) throws Exception {
        Map<String, Object> outgoingMap = new HashMap<>();
        long createTime = System.currentTimeMillis();
        outgoingMap.put(String.format("partitionKey-%d", cnt % NUM_SHARDS), String.format("testData-%d", createTime));
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
        cnt++;
    }
}
