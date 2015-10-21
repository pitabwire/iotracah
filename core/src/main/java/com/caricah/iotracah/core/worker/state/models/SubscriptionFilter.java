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

    @QuerySqlField(index = true)
    private String id;

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "partition_qos_topicfilter_idx", order = 0)})
    private String partition;

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "partition_qos_topicfilter_idx", order = 3)})
    private String topicFilter;

    @QuerySqlField(orderedGroups={@QuerySqlField.Group(
            name = "partition_qos_topicfilter_idx", order = 2)})
    private int qos;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }


    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException {

        if(null == getId()){
            throw new UnRetriableException(" id has to be set before you use the subscription filter");
        }

        return getId();
    }
}
