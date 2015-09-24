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
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class ConnectAcknowledgeMessage extends IOTMessage {

    public static final String MESSAGE_TYPE = "CONNACK";

    private final boolean dup;
    private final int qos;
    private final boolean retain;
    private MqttConnectReturnCode returnCode;


    public static ConnectAcknowledgeMessage from(boolean dup, int qos, boolean retain, MqttConnectReturnCode returnCode) {
        return new ConnectAcknowledgeMessage(dup, qos, retain, returnCode);
    }

    private ConnectAcknowledgeMessage(boolean dup, int qos, boolean retain, MqttConnectReturnCode returnCode) {

        setMessageType(MESSAGE_TYPE);
        this.dup = dup;
        this.qos = qos;
        this.retain = retain;
        this.returnCode = returnCode;
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

    public MqttConnectReturnCode getReturnCode() {
        return returnCode;
    }

    public void setReturnCode(MqttConnectReturnCode returnCode) {
        this.returnCode = returnCode;
    }

    @Override
    public String toString() {
        return getClass().getName() + '[' + "Qos=" + getQos() + ", ReturnCode=" + getReturnCode() + ']';
    }
}
