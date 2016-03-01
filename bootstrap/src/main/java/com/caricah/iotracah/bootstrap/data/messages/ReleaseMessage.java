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

package com.caricah.iotracah.bootstrap.data.messages;

import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class ReleaseMessage extends IOTMessage {

    public static final String MESSAGE_TYPE = "PUBREL";

    private final boolean dup;
    private final int messageId;

    public static ReleaseMessage from(int messageId, boolean dup) {
        if (messageId < 1 ) {
            throw new IllegalArgumentException("messageId: " + messageId + " (expected: > 1)");
        }
        return new ReleaseMessage(messageId, dup);
    }

    private ReleaseMessage(int messageId, boolean dup) {

        setMessageType(MESSAGE_TYPE);
        this.messageId = messageId;
        this.dup = dup;

    }

    public boolean isDup() {
        return dup;
    }

    public int getMessageId() {
        return messageId;
    }

    public int getQos() {
        return 1;
    }

    @Override
    public String toString() {
        return getClass().getName() + '[' + "messageId=" + getMessageId() + ']';
    }
}
