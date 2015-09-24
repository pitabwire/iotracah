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
 * @version 1.0 7/6/15
 */
public final class DisconnectMessage extends IOTMessage {

    public static final String MESSAGE_TYPE = "DISCONNECT";

    private final boolean cleanDisconnect;

    private final boolean dup;
    private final int qos;
    private final boolean retain;

    public DisconnectMessage(boolean cleanDisconnect, boolean dup, int qos, boolean retain) {

        setMessageType(MESSAGE_TYPE);
        this.cleanDisconnect = cleanDisconnect;
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;
    }



    public static DisconnectMessage from( boolean isCleanDisconnect, boolean dup, int qos, boolean retain) {
        return new DisconnectMessage(isCleanDisconnect, dup, qos, retain);


    }

    public boolean isCleanDisconnect() {
        return cleanDisconnect;
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
}
