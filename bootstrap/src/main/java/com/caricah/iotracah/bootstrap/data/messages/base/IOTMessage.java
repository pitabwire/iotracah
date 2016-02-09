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

import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.*;
import java.util.UUID;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/1/15
 */
public class IOTMessage implements Externalizable {

    private UUID nodeId;
    private String cluster;
    private String authKey;
    private Serializable connectionId;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "sessionid_msgid_inbound_idx", order = 3)
    })
    private long messageId;

    private String messageType;

    private Protocol protocol;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "sessionid_msgid_inbound_idx", order = 1)
    })
    private String sessionId;

   public Serializable getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Serializable connectionId) {
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

    public Long getMessageId() {
        return messageId;
    }

    public void setMessageId(long messageId) {
        this.messageId = messageId;
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

    public void copyBase(IOTMessage iotMessage) {

        setProtocol(iotMessage.getProtocol());
        setSessionId(iotMessage.getSessionId());
        setAuthKey(iotMessage.getAuthKey());
        setConnectionId(iotMessage.getConnectionId());
        setNodeId(iotMessage.getNodeId());
        setCluster(iotMessage.getCluster());

    }


    @Override
    public String toString() {
        return getClass().getName() + '[' + "messageId=" + getMessageId() + ']';
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(getAuthKey());
        objectOutput.writeObject(getCluster());
        objectOutput.writeObject(getConnectionId());
        objectOutput.writeLong(getMessageId());
        objectOutput.writeObject(getMessageType());
        objectOutput.writeObject(getNodeId());
        objectOutput.writeObject(getProtocol());
        objectOutput.writeObject(getSessionId());
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setAuthKey((String) objectInput.readObject());
        setCluster((String) objectInput.readObject());
        setConnectionId((Serializable) objectInput.readObject());
        setMessageId(objectInput.readLong());
        setMessageType((String) objectInput.readObject());
        setNodeId((UUID) objectInput.readObject());
        setProtocol((Protocol) objectInput.readObject());
        setSessionId((String) objectInput.readObject());



    }
}
