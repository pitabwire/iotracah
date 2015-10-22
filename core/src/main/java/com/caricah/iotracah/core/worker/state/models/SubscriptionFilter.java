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

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/30/15
 */
public class SubscriptionFilter implements Serializable, IdKeyComposer {

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_parentid_name_idx", order = 0)
    })
    private String partition;

    @QuerySqlField(index = true)
    private long id;

    @QuerySqlField(orderedGroups={
            @QuerySqlField.Group(name = "partition_parentid_name_idx", order = 2)
    })
    private long parentId;

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

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
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

        if(0 == getId()){
            throw new UnRetriableException(" id has to be set before you use the subscription filter");
        }

        return getId();
    }


    @Override
    public String toString() {
        return getClass().getSimpleName() + " [ " + " partition = " + getPartition() + "," + " parent = " + getParentId() + "," + " FullTree = " + getFullTreeName() + "," + " ]";
    }
}
