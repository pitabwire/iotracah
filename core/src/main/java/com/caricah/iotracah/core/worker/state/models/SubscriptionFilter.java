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
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/30/15
 */
public class SubscriptionFilter implements IdKeyComposer, Externalizable {

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_parentid_name_idx", order = 0)
    })
    private String partition;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_parentid_name_idx", order = 2)
    })
    private String parentId;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_parentid_name_idx", order = 3)
    })
    private String name;

    private String fullTreeName;


    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFullTreeName() {
        return fullTreeName;
    }

    public void setFullTreeName(String fullTreeName) {
        this.fullTreeName = fullTreeName;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException {

        if(Objects.isNull(getName()) || Objects.isNull(getParentId()) ){
            throw new UnRetriableException(" parent id and name have to be set before you use the subscription filter");
        }

        return getParentId()+":"+getName();
    }

    public static String quickCheckIdKey(String partition, List<String> nameParts){
        return getPartitionAsInitialParentId(partition) +":"+ String.join(":", nameParts);
    }

    public static String getPartitionAsInitialParentId(String partition) {
        return "p[" + partition + "]";
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [ " + " partition = " + getPartition() + ", parent = " + getParentId() + ", name = " + getName() + ", FullTree = " + getFullTreeName() + "," + " ]";
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(getPartition());
        objectOutput.writeObject(getParentId());
        objectOutput.writeObject(getName());
        objectOutput.writeObject(getFullTreeName());

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        setPartition((String) objectInput.readObject());
        setParentId((String) objectInput.readObject());
        setName((String) objectInput.readObject());
        setFullTreeName((String) objectInput.readObject());
    }
}
