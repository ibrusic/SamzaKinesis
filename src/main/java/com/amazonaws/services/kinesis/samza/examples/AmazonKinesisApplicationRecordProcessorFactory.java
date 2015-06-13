package com.amazonaws.services.kinesis.samza.examples;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Used to create new record processors.
 */
public class AmazonKinesisApplicationRecordProcessorFactory implements IRecordProcessorFactory {

    // Class logger
    private static final Log LOG = LogFactory.getLog(AmazonKinesisApplicationRecordProcessorFactory.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        LOG.debug("Creating processor from " + this.getClass().getSimpleName());
        System.out.println("Creating processor");
        return new AmazonKinesisApplicationSampleRecordProcessor();
    }
}