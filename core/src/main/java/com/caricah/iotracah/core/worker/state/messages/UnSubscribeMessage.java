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

import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class UnSubscribeMessage extends IOTMessage {

    public static final String MESSAGE_TYPE = "UNSUBSCRIBE";

    private final boolean dup;
    private final int qos;
    private final boolean retain;

    private final List<String> topicFilterList;


    public static UnSubscribeMessage from(long messageId, boolean dup, int qos, boolean retain, List<String> topicFilterList) {
        if (messageId < 1 ) {
            throw new IllegalArgumentException("messageId: " + messageId + " (expected: > 1)");
        }
        return new UnSubscribeMessage(messageId, dup, qos, retain, topicFilterList);
    }

    private UnSubscribeMessage(long messageId, boolean dup, int qos, boolean retain, List<String> topicFilterList) {

        setMessageType(MESSAGE_TYPE);
        setMessageId(messageId);
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;
        this.topicFilterList = topicFilterList;

    }

    public boolean isDup() {
        return dup;
    }

    public int getQos() {
        return qos;
    }

    public boolean isRetain() {
        return retain;
    }



    public List<String> getTopicFilterList() {
        return topicFilterList;
    }
}
