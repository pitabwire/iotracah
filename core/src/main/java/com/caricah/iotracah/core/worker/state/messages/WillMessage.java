/*
 *
 * Copyright (c) 2015 Caricah <info@caricah.com>.
 *
 * Caricah licenses this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License. You may obtain a copy
 *  of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under
 *  the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 *  OF ANY  KIND, either express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 *
 *
 *
 */

package com.caricah.iotracah.core.worker.state.messages;

import com.caricah.iotracah.core.worker.state.IdKeyComposer;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class WillMessage extends IOTMessage implements IdKeyComposer{

    public static final String MESSAGE_TYPE = "WILL";

    private final boolean retain;
    private final int qos;
    private final String topic;

    public boolean isRetain() {
        return retain;
    }


    public int getQos() {
        return qos;
    }

    public String getTopic() {
        return topic;
    }

    private WillMessage(boolean retain, int qos, String topic, String payload) {

        setMessageType(MESSAGE_TYPE);
        this.retain = retain;
        this.qos = qos;
        this.topic = topic;
        setPayload(payload);
    }

    public static WillMessage from(boolean retain, int qos, String topic, String payload) {
        return new WillMessage(retain, qos, topic, payload);
    }

    @Override
    public Serializable generateIdKey() {
        return null;
    }
}
