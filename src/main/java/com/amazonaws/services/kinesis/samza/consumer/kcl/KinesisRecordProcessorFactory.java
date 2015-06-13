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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Simple factory class to generate a standalone Kinesis Aggregator
 * IRecordProcessor for the application
 */
public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    private static Map<String, Object> createdProcessors = new HashMap<String, Object>();

    private final Log LOG = LogFactory.getLog(KinesisRecordProcessorFactory.class);

    private AbstractKinesisRecordProcessor managedProcessor;

    public KinesisRecordProcessorFactory(AbstractKinesisRecordProcessor managedProcessor) {
        System.out.println("Factory created");
        this.managedProcessor = managedProcessor;
    }

    /**
     * {@inheritDoc}
     */
    public IRecordProcessor createProcessor() {
        System.out.println("Creating processor");
        try {

            LOG.info("Creating new Managed Client Processor");
            AbstractKinesisRecordProcessor p = this.managedProcessor.copy();
            createdProcessors.put(p.toString(), p);
            return p;
        } catch (Exception e) {
            LOG.error(e);
            return null;
        }
    }

    public void notifyOfShutdown(IRecordProcessor processor) {
        LOG.info(String.format("Received Shutdown Notification for AggregatorProcessor %s",
                processor.toString()));
        createdProcessors.remove(processor.toString());
    }
}
