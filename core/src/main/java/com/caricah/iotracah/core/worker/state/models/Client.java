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

package com.caricah.iotracah.core.worker.state.models;


import com.caricah.iotracah.data.IdKeyComposer;
import com.caricah.iotracah.core.worker.state.messages.*;
import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.worker.state.Messenger;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import rx.Observable;

import java.io.*;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class Client implements IdKeyComposer, Externalizable {

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "partition_clientid_idx", order = 0)})
    private String partition;

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "partition_clientid_idx", order = 2)})
    private String clientId;

    @QuerySqlField()
    private Serializable sessionId;

    @QuerySqlField()
    private String connectedCluster;

    @QuerySqlField()
    private UUID connectedNode;

    @QuerySqlField()
    private Serializable connectionId;

    @QuerySqlField()
    private boolean active;

    private boolean cleanSession;

    private Protocal protocal;

    private String protocalData;

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Serializable getSessionId() {
        return sessionId;
    }

    public void setSessionId(Serializable sessionId) {
        this.sessionId = sessionId;
    }

    public UUID getConnectedNode() {
        return connectedNode;
    }

    public void setConnectedNode(UUID connectedNode) {
        this.connectedNode = connectedNode;
    }

    public String getConnectedCluster() {
        return connectedCluster;
    }

    public void setConnectedCluster(String connectedCluster) {
        this.connectedCluster = connectedCluster;
    }

    public Serializable getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Serializable connectionId) {
        this.connectionId = connectionId;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public Protocal getProtocal() {
        return protocal;
    }

    public void setProtocal(Protocal protocal) {
        this.protocal = protocal;
    }

    public String getProtocalData() {
        return protocalData;
    }

    public void setProtocalData(String protocalData) {
        this.protocalData = protocalData;
    }

    /**

     */
    public void internalPublishMessage(Messenger messenger, PublishMessage publishMessage) throws RetriableException {

        messenger.publish(getPartition(), publishMessage);
    }


    public Observable<WillMessage> getWill(Datastore datastore) {
        String willKey = WillMessage.getWillKey(getPartition(), getClientId());
        return datastore.getWill(willKey);
    }


    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if(Objects.isNull(getClientId())){
            throw new UnRetriableException(" Client Id has to be non null");
        }

        return createIdKey(getPartition(), getClientId());
    }


    public static Serializable createIdKey(String partition, String clientId) {

        return "["+ partition+"]"+ clientId;

    }



    public <T extends IOTMessage> T copyTransmissionData(T iotMessage) {

        iotMessage.setSessionId(getSessionId());
        iotMessage.setProtocal(getProtocal());
        iotMessage.setConnectionId(getConnectionId());
        iotMessage.setNodeId(getConnectedNode());
        iotMessage.setCluster(getConnectedCluster());

        if(iotMessage instanceof PublishMessage) {
            PublishMessage publishMessage = (PublishMessage) iotMessage;
            publishMessage.setPartition(getPartition());
            publishMessage.setClientId(getClientId());
        }



        return iotMessage;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        objectOutput.writeObject(getClientId());
        objectOutput.writeObject(getConnectedCluster());
        objectOutput.writeObject(getConnectedNode());
        objectOutput.writeObject(getConnectionId());
        objectOutput.writeObject(getPartition());
        objectOutput.writeObject(getProtocal());
        objectOutput.writeObject(getProtocalData());
        objectOutput.writeObject(getSessionId());
        objectOutput.writeBoolean(isActive());
        objectOutput.writeBoolean(isCleanSession());
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setClientId((String) objectInput.readObject());
        setConnectedCluster((String) objectInput.readObject());
        setConnectedNode((UUID) objectInput.readObject());
        setConnectionId((Serializable) objectInput.readObject());
        setPartition((String) objectInput.readObject());
        setProtocal((Protocal) objectInput.readObject());
        setProtocalData((String) objectInput.readObject());
        setSessionId((Serializable) objectInput.readObject());
        setActive(objectInput.readBoolean());
        setCleanSession(objectInput.readBoolean());

    }
}
