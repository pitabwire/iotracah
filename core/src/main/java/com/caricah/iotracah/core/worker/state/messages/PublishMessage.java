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

import com.caricah.iotracah.core.worker.state.IdKeyComposer;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.Serializable;
import java.util.Locale;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class PublishMessage extends IOTMessage implements IdKeyComposer {

    public static final String MESSAGE_TYPE = "PUBLISH";

    @QuerySqlField()
    private int qos;

    @QuerySqlField()
    private boolean retain;

    @QuerySqlField()
    private boolean dup;

    @QuerySqlField()
    private String topic;

    @QuerySqlField()
    private boolean released;

    @QuerySqlField()
    private boolean inBound;

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public boolean isRetain() {
        return retain;
    }

    public void setRetain(boolean retain) {
        this.retain = retain;
    }

    public boolean isDup() {
        return dup;
    }

    public void setDup(boolean dup) {
        this.dup = dup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isReleased() {
        return released;
    }

    public void setReleased(boolean released) {
        this.released = released;
    }

    public boolean isInBound() {
        return inBound;
    }

    public void setInBound(boolean inBound) {
        this.inBound = inBound;
    }

    public static PublishMessage from( long messageId, boolean dup, int qos, boolean retain, String topic, Serializable payload, boolean inBound) {

        PublishMessage publishMessage = new PublishMessage();
        publishMessage.setMessageType(MESSAGE_TYPE);
        publishMessage.setQos(qos);
        publishMessage.setRetain(retain);
        publishMessage.setDup(dup);
        publishMessage.setTopic(topic);
        publishMessage.setMessageId(messageId);
        publishMessage.setPayload(payload);
        publishMessage.setInBound(inBound);

        return publishMessage;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if (null == getClientIdentifier() || getMessageId() <= 0) {
            throw new UnRetriableException(" Messages are stored only if they have an owner and an Id");
        }

        return String.format(Locale.US, "%s-%s-%d", getClientIdentifier(), isInBound() ? "i" : "o", getMessageId());
    }

    public PublishMessage cloneMessage() {

        PublishMessage publishMessage = PublishMessage.from(getMessageId(), false, getQos(), isRetain(),  getTopic(), getPayload(), false);
        publishMessage.setMessageType(getMessageType());
        publishMessage.setProtocal(getProtocal());

        return publishMessage;
    }
}
