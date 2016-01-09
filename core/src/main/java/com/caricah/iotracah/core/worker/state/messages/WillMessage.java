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

import com.caricah.iotracah.data.IdKeyComposer;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class WillMessage extends IOTMessage implements IdKeyComposer{

    public static final String MESSAGE_TYPE = "WILL";

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_clientid_idx", order = 0)
    })
    private String partition;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_clientid_idx", order = 1)
    })
    private String clientId;

    private boolean retain;
    private int qos;
    private String topic;
    private Serializable payload;


    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Serializable getPayload() {
        return payload;
    }

    public void setPayload(Serializable payload) {
        this.payload = payload;
    }


    private WillMessage() {
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

    public PublishMessage toPublishMessage(){

        byte[] willPayloadBytes = ((String) getPayload()).getBytes();
        ByteBuffer willByteBuffer = ByteBuffer.wrap(willPayloadBytes);

        long messageId = 1;
        if(getQos() > 0 ){
            messageId = PublishMessage.ID_TO_FORCE_GENERATION_ON_SAVE;
        }

        //TODO: generate sequence for will message id
        return PublishMessage.from(
                messageId, false, getQos(),
                isRetain(), getTopic(),
                willByteBuffer, true
        );
    }

    public static String getWillKey(String partition, String clientId){

        return String.format("%s-%s", partition, clientId);
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if(null == getClientId()){
            throw new UnRetriableException(" Client Id has to be non null");
        }

        return WillMessage.getWillKey(getPartition(), getClientId());

    }


    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        objectOutput.writeObject(getClientId());
        objectOutput.writeObject(getPartition());
        objectOutput.writeObject(getPayload());
        objectOutput.writeInt(getQos());
        objectOutput.writeObject(getTopic());
        objectOutput.writeBoolean(isRetain());

        super.writeExternal(objectOutput);


    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setClientId((String) objectInput.readObject());
        setPartition((String) objectInput.readObject());
        setPayload((Serializable) objectInput.readObject());
        setQos(objectInput.readInt());
        setTopic((String) objectInput.readObject());
        setRetain(objectInput.readBoolean());

        super.readExternal(objectInput);
    }

}
