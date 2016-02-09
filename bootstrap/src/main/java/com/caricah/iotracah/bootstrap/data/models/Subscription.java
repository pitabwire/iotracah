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

package com.caricah.iotracah.bootstrap.data.models;

import com.caricah.iotracah.bootstrap.data.IdKeyComposer;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/20/15
 */
public class Subscription implements IdKeyComposer, Externalizable {

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_topicfilterkey_qos_idx", order = 0)
    })
    private String partition;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_topicfilterkey_qos_idx", order = 2)
    })
    private String topicFilterKey;

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "partition_topicfilterkey_qos_idx", order = 5)})
    private int qos;


    @QuerySqlField(index = true)
    private String sessionId;

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getTopicFilterKey() {
        return topicFilterKey;
    }

    public void setTopicFilterKey(String topicFilterKey) {
        this.topicFilterKey = topicFilterKey;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException {

        return getSessionId()+"-"+ getTopicFilterKey();
    }


    @Override
    public String toString() {
        return getClass().getSimpleName()+getSessionId()+", topicFilterKey="+getTopicFilterKey()+", qos="+getQos()+ "]";
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(getPartition());
        objectOutput.writeObject(getSessionId());
        objectOutput.writeInt(getQos());
        objectOutput.writeObject(getTopicFilterKey());

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setPartition((String) objectInput.readObject());
        setSessionId((String) objectInput.readObject());
        setQos(objectInput.readInt());
        setTopicFilterKey((String)objectInput.readObject());
    }
}
