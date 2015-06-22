/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.samza.producer;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.kinesis.samza.KinesisUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.amazonaws.services.kinesis.samza.KinesisUtils.getClient;
import static com.amazonaws.services.kinesis.samza.Constants.*;

/**
 * Class to produce streams into Kinesis' streams
 */
public class KinesisSystemProducer implements SystemProducer {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSystemProducer.class);

    private final List<String> streams;
    private AmazonKinesisClient kinesis;
    private final boolean autoCreate;
    private final int numShards;


    public KinesisSystemProducer(String systemName, Config config) {
        String credentialsPath = config.get(String.format("systems.%s.%s", systemName, CONFIG_PATH_PARAM));
        String region = config.get(String.format("systems.%s.%s", systemName, AWS_REGION_PARAM));
        this.autoCreate = config.getBoolean(String.format("systems.%s.%s", systemName, AUTO_CREATE_STREAM), false);
        this.numShards = config.getInt(String.format("systems.%s.%s", systemName, NUMBER_SHARD), DEFAULT_NUM_SHARDS);
        this.kinesis = getClient(credentialsPath, region);
        this.streams = new ArrayList<>();
    }

    @Override
    public void start() {
        //TODO
    }

    @Override
    public void stop() {
        //TODO
    }

    @Override
    public void register(String stream) {
        this.streams.add(stream);
        if (this.autoCreate) {
            try {
                if (KinesisUtils.checkOrCreate(stream, kinesis, numShards))
                    LOG.debug(stream + " successfully created.");
                else
                    LOG.debug(stream + " was not successfully created.");
            } catch (InterruptedException e) {
                LOG.error("Something went wrong while trying to create " + stream);
                e.printStackTrace();
            }
        }
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope outgoingMessageEnvelope) {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(source);
        //putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d", createTime).getBytes()));
        putRecordRequest.setData(ByteBuffer.wrap(outgoingMessageEnvelope.getMessage().toString().getBytes()));
        //putRecordRequest.setPartitionKey(String.format("partitionKey-%d", cnt % NUM_SHARDS));
        putRecordRequest.setPartitionKey(outgoingMessageEnvelope.getKey().toString());

        PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
        LOG.debug(String.format("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                putRecordRequest.getPartitionKey(),
                putRecordResult.getShardId(),
                putRecordResult.getSequenceNumber()));
    }

    @Override
    public void flush(String s) {
        //TODO
    }
}