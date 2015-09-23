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
public final class SubscribeMessage extends IOTMessage {

    private final boolean dup;
    private final int qos;
    private final boolean retain;

    private final List<Map.Entry<String,Integer>> topicFilterList = new ArrayList<>();


    public static SubscribeMessage from(long messageId, boolean dup, int qos, boolean retain) {
        if (messageId < 1 ) {
            throw new IllegalArgumentException("messageId: " + messageId + " (expected: > 1)");
        }
        return new SubscribeMessage(messageId, dup, qos, retain);
    }

    private SubscribeMessage(long messageId, boolean dup, int qos, boolean retain) {

        setMessageId(messageId);
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;

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



    public List<Map.Entry<String,Integer>> getTopicFilterList() {
        return topicFilterList;
    }
}
