/**
 * Amazon Kinesis Aggregators Copyright 2014, Amazon.com, Inc. or its
 * affiliates. All Rights Reserved. Licensed under the Amazon Software License
 * (the "License"). You may not use this file except in compliance with the
 * License. A copy of the License is located at http://aws.amazon.com/asl/ or in
 * the "license" file accompanying this file. This file is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.amazonaws.services.kinesis.samza.consumer.kcl;

import java.util.List;

import com.amazonaws.services.kinesis.samza.consumer.KinesisSystemConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import static com.amazonaws.services.kinesis.samza.Constants.*;

/**
 * Abstract class for defining a Kinesis record processor.
 */
public abstract class AbstractKinesisRecordProcessor implements IRecordProcessor {
    // Logger for KinesisRecordProcessors
    protected static final Log LOG = LogFactory.getLog(AbstractKinesisRecordProcessor.class);

    // Kinesis shardId to be processed
    protected String kinesisShardId;

    // Factory the processor was created by
    private IRecordProcessorFactory createdByFactory;

    // Samza's consumer to send messages to
    protected KinesisSystemConsumer consumer;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract void processRecords(List<Record> records,
                                        IRecordProcessorCheckpointer checkpointer);

    /**
     * Copies the record processor
     *
     * @return
     * @throws Exception
     */
    public abstract AbstractKinesisRecordProcessor copy() throws Exception;

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing Managed Processor for Shard: " + shardId);
        this.kinesisShardId = shardId;
        this.consumer.registerProcessor(shardId, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);

        // notify the factory that created this processor of shutdown
        if (this.createdByFactory instanceof KinesisRecordProcessorFactory) {
            ((KinesisRecordProcessorFactory) this.createdByFactory).notifyOfShutdown(this);
        }

        // Important to checkpoint after reaching end of shard, so we can start
        // processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            try {
                checkpoint(checkpointer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Checkpoint with retries.
     *
     * @param checkpointer
     */
    protected void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < DEFAULT_NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown
                // (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (DEFAULT_NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + DEFAULT_NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for
                // table, provisioned IOPS).
                LOG.error(
                        "Cannot save checkpoint to DynamoDB table used by the KinesisClientLibrary.",
                        e);
                break;
            }
            try {
                Thread.sleep(DEFAULT_BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }

    /**
     * Sets the processor factory
     *
     * @param factory
     */
    public void setCreatedByFactory(IRecordProcessorFactory factory) {
        this.createdByFactory = factory;
    }
}
