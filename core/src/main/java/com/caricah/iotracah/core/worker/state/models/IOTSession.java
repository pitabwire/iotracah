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

import com.caricah.iotracah.core.worker.state.session.SerializableUtils;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.shiro.session.Session;

import java.io.Serializable;
import java.util.Date;

/**
 *
 * Backing class expected to effect :
 *
 *          select * from sessions s where s.lastAccessTimestamp < ? and s.stopTimestamp is null
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/9/15
 */
public class IOTSession implements Serializable {


    @QuerySqlField()
    private Date lastAccessTimestamp;

    private String sessionString;

    public IOTSession(Session session) {

        lastAccessTimestamp = session.getLastAccessTime();

        sessionString = SerializableUtils.serialize(session);
    }

    public Date getLastAccessTimestamp() {
        return lastAccessTimestamp;
    }

    public void setLastAccessTimestamp(Date lastAccessTimestamp) {
        this.lastAccessTimestamp = lastAccessTimestamp;
    }

    public String getSessionString() {
        return sessionString;
    }

    public void setSessionString(String sessionString) {
        this.sessionString = sessionString;
    }

    public Session toSession() {
        return SerializableUtils.deserialize(getSessionString());
    }
}
