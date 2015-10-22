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

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class WillMessage extends IOTMessage implements IdKeyComposer{

    public static final String MESSAGE_TYPE = "WILL";

    @QuerySqlField(index = true)
    private String partition;

    @QuerySqlField(index = true)
    private String clientId;

    private final boolean retain;
    private final int qos;
    private final String topic;
    private Serializable payload;


    public boolean isRetain() {
        return retain;
    }


    public int getQos() {
        return qos;
    }

    public String getTopic() {
        return topic;
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

        //TODO: generate sequence for will message id
        return PublishMessage.from(
                getMessageId(), false, getQos(),
                isRetain(), getTopic(),
                willByteBuffer, true
        );
    }
    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if(null == getClientId()){
            throw new UnRetriableException(" Client Id has to be non null");
        }

        return String.format("%s-%s", getPartition(), getClientId());

    }
}
