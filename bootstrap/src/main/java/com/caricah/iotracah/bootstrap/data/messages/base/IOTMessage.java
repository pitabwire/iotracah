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

package com.caricah.iotracah.bootstrap.data.messages.base;

import java.io.Serializable;
import java.util.UUID;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/1/15
 */
public class IOTMessage implements Serializable {

    private UUID nodeId;
    private String cluster;
    private String authKey;
    private String connectionId;
    private String sessionId;

    private String messageType;

    private Protocol protocol;

   public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    public String getAuthKey() {
        return authKey;
    }

    public void setAuthKey(String authKey) {
        this.authKey = authKey;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getNodeId() {
        return nodeId;
    }

    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public  <T extends IOTMessage>  void copyTransmissionData( T bossMessage ) {

        setSessionId(bossMessage.getSessionId());
        setProtocol(bossMessage.getProtocol());
        setConnectionId(bossMessage.getConnectionId());
        setNodeId(bossMessage.getNodeId());
        setCluster(bossMessage.getCluster());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '['
                + "connectionId=" + getConnectionId() +","
                + "sessionId=" + getSessionId() +","
                +  ']';
    }
}
