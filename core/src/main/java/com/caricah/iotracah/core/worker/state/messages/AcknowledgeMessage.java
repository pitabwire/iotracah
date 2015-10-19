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

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class AcknowledgeMessage extends IOTMessage {

    public static final String MESSAGE_TYPE = "PUBACK";

    private final boolean dup;
    private final int qos = 0;
    private final boolean retain;
    private final boolean inBound;

    public static AcknowledgeMessage from(long messageId, boolean dup, boolean retain, boolean inBound) {
        if (messageId < 1 ) {
            throw new IllegalArgumentException("messageId: " + messageId + " (expected: > 1)");
        }
        return new AcknowledgeMessage(messageId, dup, retain, inBound);
    }

    private AcknowledgeMessage(long messageId, boolean dup, boolean retain, boolean inBound) {

        setMessageId(messageId);
        setMessageType(MESSAGE_TYPE);
        this.dup = dup;
        this.retain = retain;
        this.inBound = inBound;

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

    public boolean isInBound() {
        return inBound;
    }

    @Override
    public String toString() {
        return new StringBuilder(getClass().getName())
                .append('[')
                .append("messageId=").append(getMessageId())
                .append(']')
                .toString();
    }
}
