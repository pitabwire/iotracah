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

import com.caricah.iotracah.bootstrap.data.IdKeyComposer;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public final class PublishMessage extends IOTMessage implements IdKeyComposer {

    public static final String MESSAGE_TYPE = "PUBLISH";

    public static final long ID_TO_FORCE_GENERATION_ON_SAVE = -517715;

    @QuerySqlField()
    private String partition;

    @QuerySqlField(index = true)
    private long id;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "sessionid_msgid_inbound_idx", order = 6)
    })
    private boolean inBound;

    private int qos;

    private boolean retain;

    private boolean dup;

    private String topic;

    private boolean released;


    private Serializable payload;

    private String protocalData;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

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

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public Serializable getPayload() {
        return payload;
    }

    public void setPayload(Serializable payload) {
        this.payload = payload;
    }

    public String getProtocalData() {
        return protocalData;
    }

    public void setProtocalData(String protocalData) {
        this.protocalData = protocalData;
    }

    public static PublishMessage from( long messageId, boolean dup, int qos, boolean retain, String topic, ByteBuffer payloadBuffer, boolean inBound) {

        if (messageId < 1 && messageId != ID_TO_FORCE_GENERATION_ON_SAVE) {


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
        publishMessage.setRetain(retain);
        publishMessage.setDup(dup);
        publishMessage.setTopic(topic);
        publishMessage.setMessageId(messageId);

        publishMessage.setPayload(toBytes(payloadBuffer));
        publishMessage.setInBound(inBound);

        return publishMessage;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if (Objects.isNull( getSessionId()) || getMessageId() <= 0) {
            throw new UnRetriableException(" Messages are stored only if they have an owner and a global Id");
        }

        return getSessionId()+"-"+getMessageId();
    }

    public PublishMessage cloneMessage() {

        ByteBuffer byteBuffer = ByteBuffer.wrap((byte[])getPayload());

        long messageId ;
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
                + "partition=" + getPartition() +","
                + "sessionId=" + getSessionId() +","
                + "id=" + getId() +","
                + "messageId=" + getMessageId() +","
                + "topic=" + getTopic() +","
                + "qos=" + getQos() +","
                +  ']';
    }


    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

        objectOutput.writeObject(getSessionId());
        objectOutput.writeLong(getId());
        objectOutput.writeLong(getMessageId());
        objectOutput.writeObject(getPartition());
        objectOutput.writeObject(getPayload());
        objectOutput.writeInt(getQos());
        objectOutput.writeObject(getTopic());
        objectOutput.writeBoolean(isDup());
        objectOutput.writeBoolean(isInBound());
        objectOutput.writeBoolean(isReleased());
        objectOutput.writeBoolean(isRetain());

        super.writeExternal(objectOutput);


    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setSessionId((String) objectInput.readObject());
        setId(objectInput.readLong());
        setMessageId(objectInput.readLong());
        setPartition((String) objectInput.readObject());
        setPayload((Serializable) objectInput.readObject());
        setQos(objectInput.readInt());
        setTopic((String) objectInput.readObject());
        setDup(objectInput.readBoolean());
        setInBound(objectInput.readBoolean());
        setReleased(objectInput.readBoolean());
        setRetain(objectInput.readBoolean());

        super.readExternal(objectInput);
    }
}
