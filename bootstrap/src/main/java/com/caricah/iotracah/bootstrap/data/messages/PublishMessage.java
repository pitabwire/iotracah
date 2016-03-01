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
import com.caricah.iotracah.bootstrap.data.models.messages.IotMessageKey;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class PublishMessage extends IOTMessage {

    public static final String MESSAGE_TYPE = "PUBLISH";

    public static final int ID_TO_FORCE_GENERATION_ON_SAVE = -5;
    public static final int ID_TO_SHOW_IS_WILL = -11;

    private String protocolData;

    /** */
    private static final long serialVersionUID = 0L;

    /** Value for dateCreated. */
    private java.sql.Timestamp dateCreated;

    /** Value for dateModified. */
    private java.sql.Timestamp dateModified;

    /** Value for isActive. */
    private boolean isActive;

    /** Value for id. */
    private long id;

    /** Value for messageId. */
    private int messageId;

    /** Value for topic. */
    private String topic;

    /** Value for payload. */
    private Object payload;

    /** Value for qos. */
    private int qos;

    /** Value for isInbound. */
    private boolean isInbound;

    /** Value for isDuplicate. */
    private boolean isDuplicate;

    /** Value for isRetain. */
    private boolean isRetain;

    /** Value for isRelease. */
    private boolean isRelease;

    /** Value for isWill. */
    private boolean isWill;

    /** Value for clientId. */
    private String clientId;

    /** Value for partitionId. */
    private String partitionId;

    /**
     * Gets dateCreated.
     *
     * @return Value for dateCreated.
     */
    public java.sql.Timestamp getDateCreated() {
        return dateCreated;
    }

    /**
     * Sets dateCreated.
     *
     * @param dateCreated New value for dateCreated.
     */
    public void setDateCreated(java.sql.Timestamp dateCreated) {
        this.dateCreated = dateCreated;
    }

    /**
     * Gets dateModified.
     *
     * @return Value for dateModified.
     */
    public java.sql.Timestamp getDateModified() {
        return dateModified;
    }

    /**
     * Sets dateModified.
     *
     * @param dateModified New value for dateModified.
     */
    public void setDateModified(java.sql.Timestamp dateModified) {
        this.dateModified = dateModified;
    }

    /**
     * Gets isActive.
     *
     * @return Value for isActive.
     */
    public boolean getIsActive() {
        return isActive;
    }

    /**
     * Sets isActive.
     *
     * @param isActive New value for isActive.
     */
    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public long getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Gets messageId.
     *
     * @return Value for messageId.
     */
    public int getMessageId() {
        return messageId;
    }

    /**
     * Sets messageId.
     *
     * @param messageId New value for messageId.
     */
    public void setMessageId(int messageId) {
        this.messageId = messageId;
    }

    /**
     * Gets topic.
     *
     * @return Value for topic.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets topic.
     *
     * @param topic New value for topic.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Gets payload.
     *
     * @return Value for payload.
     */
    public Object getPayload() {
        return payload;
    }

    /**
     * Sets payload.
     *
     * @param payload New value for payload.
     */
    public void setPayload(Object payload) {
        this.payload = payload;
    }

    /**
     * Gets qos.
     *
     * @return Value for qos.
     */
    public int getQos() {
        return qos;
    }

    /**
     * Sets qos.
     *
     * @param qos New value for qos.
     */
    public void setQos(int qos) {
        this.qos = qos;
    }

    /**
     * Gets isInbound.
     *
     * @return Value for isInbound.
     */
    public boolean getIsInbound() {
        return isInbound;
    }

    /**
     * Sets isInbound.
     *
     * @param isInbound New value for isInbound.
     */
    public void setIsInbound(boolean isInbound) {
        this.isInbound = isInbound;
    }

    /**
     * Gets isDuplicate.
     *
     * @return Value for isDuplicate.
     */
    public boolean getIsDuplicate() {
        return isDuplicate;
    }

    /**
     * Sets isDuplicate.
     *
     * @param isDuplicate New value for isDuplicate.
     */
    public void setIsDuplicate(boolean isDuplicate) {
        this.isDuplicate = isDuplicate;
    }

    /**
     * Gets isRetain.
     *
     * @return Value for isRetain.
     */
    public boolean getIsRetain() {
        return isRetain;
    }

    /**
     * Sets isRetain.
     *
     * @param isRetain New value for isRetain.
     */
    public void setIsRetain(boolean isRetain) {
        this.isRetain = isRetain;
    }

    /**
     * Gets isRelease.
     *
     * @return Value for isRelease.
     */
    public boolean getIsRelease() {
        return isRelease;
    }

    /**
     * Sets isRelease.
     *
     * @param isRelease New value for isRelease.
     */
    public void setIsRelease(boolean isRelease) {
        this.isRelease = isRelease;
    }

    /**
     * Gets isWill.
     *
     * @return Value for isWill.
     */
    public boolean getIsWill() {
        return isWill;
    }

    /**
     * Sets isWill.
     *
     * @param isWill New value for isWill.
     */
    public void setIsWill(boolean isWill) {
        this.isWill = isWill;
    }

    /**
     * Gets clientId.
     *
     * @return Value for clientId.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Sets clientId.
     *
     * @param clientId New value for clientId.
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Gets partitionId.
     *
     * @return Value for partitionId.
     */
    public String getPartitionId() {
        return partitionId;
    }

    /**
     * Sets partitionId.
     *
     * @param partitionId New value for partitionId.
     */
    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof PublishMessage))
            return false;

        PublishMessage that = (PublishMessage)o;

        if (dateCreated != null ? !dateCreated.equals(that.dateCreated) : that.dateCreated != null)
            return false;

        if (dateModified != null ? !dateModified.equals(that.dateModified) : that.dateModified != null)
            return false;

        if (isActive != that.isActive)
            return false;

        if (id != that.id)
            return false;

        if (messageId != that.messageId)
            return false;

        if (topic != null ? !topic.equals(that.topic) : that.topic != null)
            return false;

        if (payload != null ? !payload.equals(that.payload) : that.payload != null)
            return false;

        if (qos != that.qos)
            return false;

        if (isInbound != that.isInbound)
            return false;

        if (isDuplicate != that.isDuplicate)
            return false;

        if (isRetain != that.isRetain)
            return false;

        if (isRelease != that.isRelease)
            return false;

        if (isWill != that.isWill)
            return false;

        if (clientId != null ? !clientId.equals(that.clientId) : that.clientId != null)
            return false;

        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dateCreated != null ? dateCreated.hashCode() : 0;

        res = 31 * res + (dateModified != null ? dateModified.hashCode() : 0);

        res = 31 * res + (isActive ? 1 : 0);

        res = 31 * res + (int)(id ^ (id >>> 32));

        res = 31 * res + messageId;

        res = 31 * res + (topic != null ? topic.hashCode() : 0);

        res = 31 * res + (payload != null ? payload.hashCode() : 0);

        res = 31 * res + qos;

        res = 31 * res + (isInbound ? 1 : 0);

        res = 31 * res + (isDuplicate ? 1 : 0);

        res = 31 * res + (isRetain ? 1 : 0);

        res = 31 * res + (isRelease ? 1 : 0);

        res = 31 * res + (isWill ? 1 : 0);

        res = 31 * res + (clientId != null ? clientId.hashCode() : 0);

        res = 31 * res + (partitionId != null ? partitionId.hashCode() : 0);

        return res;
    }


    public String getProtocolData() {
        return protocolData;
    }

    public void setProtocolData(String protocolData) {
        this.protocolData = protocolData;
    }

    public static PublishMessage from( int messageId, boolean dup, int qos, boolean retain, String topic, ByteBuffer payloadBuffer, boolean inBound) {

        if (messageId < 1
                && messageId != ID_TO_FORCE_GENERATION_ON_SAVE
                && messageId != ID_TO_SHOW_IS_WILL) {


            if(qos == 0 ){
                //The Packet Identifier field is only present
                // in PUBLISH Packets where the QoS level is 1 or 2
                messageId = 0;
            }else
            throw new IllegalArgumentException("messageId: " + messageId + " (expected: > 1)");
        }

        if(0 > qos || qos > 2 ){
            throw new IllegalArgumentException("qos: " + qos + " (expected: 0, 1 or 2 )");
        }


        PublishMessage publishMessage = new PublishMessage();
        publishMessage.setMessageType(MESSAGE_TYPE);
        publishMessage.setQos(qos);
        publishMessage.setIsRetain(retain);
        publishMessage.setIsDuplicate(dup);
        publishMessage.setTopic(topic);
        publishMessage.setMessageId(messageId);

        publishMessage.setPayload(toBytes(payloadBuffer));
        publishMessage.setIsInbound(inBound);

        return publishMessage;
    }

    public PublishMessage cloneMessage() {

        ByteBuffer byteBuffer = ByteBuffer.wrap((byte[])getPayload());

        int messageId ;
        if(getQos() > 0 ){
           messageId = ID_TO_FORCE_GENERATION_ON_SAVE;
        }else{
            messageId = getMessageId();
        }

        PublishMessage publishMessage = PublishMessage.from(messageId, false, getQos(), false,  getTopic(), byteBuffer, false);
        publishMessage.setProtocol(getProtocol());
        publishMessage.setId(-1);

        return publishMessage;
    }


    public static IotMessageKey createMessageKey(PublishMessage publishMessage) {

        IotMessageKey key = new IotMessageKey();
        key.setPartitionId(publishMessage.getPartitionId());
        key.setClientId(publishMessage.getSessionId());
        key.setMessageId(publishMessage.getMessageId());

        return key;
    }

    /**
     * Get byte array from ByteBuffer.
     * This function returns a byte array reference that has exactly the same
     * valid range as the ByteBuffer. Note that you should not write to the
     * resulting byte array directly. If you want a writable copy, please use
     * org.apache.hadoop.hbase.util.Bytes.toBytes(ByteBuffer).
     *
     * @param bb  the byte buffer
     * @return a reference to a byte array that contains the same content as the
     *         given ByteBuffer
     */
    public static byte[] toBytes(final ByteBuffer bb) {
        // we cannot call array() on read only or direct ByteBuffers
        if (bb.isReadOnly() || bb.isDirect()) {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream(bb.limit());
                Channels.newChannel(out).write(bb);
                return out.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e); // memory error
            }
        } else if (bb.array().length == bb.limit()) {
            return bb.array();
        } else {
            return Arrays.copyOfRange(
                    bb.array(), bb.arrayOffset(), bb.arrayOffset() + bb.limit()
            );
        }
    }



    @Override
    public String toString() {
        return getClass().getSimpleName() + '['
                + "partition=" + getPartitionId() +","
                + "sessionId=" + getSessionId() +","
                + "id=" + getId() +","
                + "messageId=" + getMessageId() +","
                + "topic=" + getTopic() +","
                + "qos=" + getQos() +","
                +  ']';
    }



}
