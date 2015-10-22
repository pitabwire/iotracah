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
import com.caricah.iotracah.data.IdKeyComposer;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Locale;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class RetainedMessage extends IOTMessage implements IdKeyComposer {

    @QuerySqlField(index = true)
    private String partition;

    @QuerySqlField
    private long topicFilterId;

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

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public long getTopicFilterId() {
        return topicFilterId;
    }

    public void setTopicFilterId(long topicFilterId) {
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

        int messageId = (int)(getTopicFilterId() % (Short.MAX_VALUE * 2));

        return  PublishMessage.from(messageId, false, getQos(), false, getTopic(), byteBuffer, false);
    }
    public static RetainedMessage from(String partition, long topicFilterId, PublishMessage publishMessage) {

        RetainedMessage retainedMessage = new RetainedMessage();

        retainedMessage.setPartition(partition);
        retainedMessage.setTopicFilterId(topicFilterId);
        retainedMessage.setQos(publishMessage.getQos());
        retainedMessage.setTopic(publishMessage.getTopic());
        retainedMessage.setPayload(publishMessage.getPayload());

        return retainedMessage;
    }

    public static String createKey(String partition, long topicFilterId){
        return String.format("%s-%d", partition, topicFilterId );
    }
    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if (getTopicFilterId() <= 0 && getMessageId() <= 0) {
            throw new UnRetriableException(" Retained messages are stored only if they have a topic filter id and an Id");
        }

        return RetainedMessage.createKey(getPartition(), getTopicFilterId());
    }

}
