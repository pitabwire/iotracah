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
import com.caricah.iotracah.bootstrap.data.IdKeyComposer;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class RetainedMessage extends IOTMessage implements IdKeyComposer {

    @QuerySqlField
    private String topicFilterId;

    @QuerySqlField
    private int qos;

    @QuerySqlField
    private String topic;

    private Serializable payload;


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

    public String getTopicFilterId() {
        return topicFilterId;
    }

    public void setTopicFilterId(String topicFilterId) {
        this.topicFilterId = topicFilterId;
    }

    public Serializable getPayload() {
        return payload;
    }

    public void setPayload(Serializable payload) {
        this.payload = payload;
    }

    public PublishMessage toPublishMessage() {

        ByteBuffer byteBuffer = ByteBuffer.wrap((byte[]) getPayload());

        return  PublishMessage.from(PublishMessage.ID_TO_FORCE_GENERATION_ON_SAVE, false, getQos(), false, getTopic(), byteBuffer, false);
    }
    public static RetainedMessage from( String topicFilterId, PublishMessage publishMessage) {

        RetainedMessage retainedMessage = new RetainedMessage();
        retainedMessage.setMessageId(publishMessage.getMessageId());
        retainedMessage.setTopicFilterId(topicFilterId);
        retainedMessage.setQos(publishMessage.getQos());
        retainedMessage.setTopic(publishMessage.getTopic());
        retainedMessage.setPayload(publishMessage.getPayload());

        return retainedMessage;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if (Objects.isNull(getTopicFilterId()) && getMessageId() <= 0) {
            throw new UnRetriableException(" Retained messages are stored only if they have a topic filter id and an Id");
        }

        return getTopicFilterId();
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        objectOutput.writeObject(getPayload());
        objectOutput.writeInt(getQos());
        objectOutput.writeObject(getTopic());
        objectOutput.writeObject(getTopicFilterId());

        super.writeExternal(objectOutput);


    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setPayload((Serializable) objectInput.readObject());
        setQos(objectInput.readInt());
        setTopic((String) objectInput.readObject());
        setTopicFilterId((String) objectInput.readObject());

        super.readExternal(objectInput);
    }


}
