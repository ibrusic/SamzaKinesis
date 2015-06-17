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

import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

/**
 * Class to produce streams into Kinesis' streams
 */
public class KinesisSystemProducer implements SystemProducer {
    @Override
    public void start() {
        //TODO
    }

    @Override
    public void stop() {
        //TODO
    }

    @Override
    public void register(String s) {
        //TODO
    }

    @Override
    public void send(String s, OutgoingMessageEnvelope outgoingMessageEnvelope) {
        //TODO
    }

    @Override
    public void flush(String s) {
        //TODO
    }
}