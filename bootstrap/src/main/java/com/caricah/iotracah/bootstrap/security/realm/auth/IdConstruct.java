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

package com.caricah.iotracah.bootstrap.security.realm.auth;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.Objects;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IdConstruct implements Serializable {


    /**
     * The partition
     */
    private final String partition;

    /**
     * The username
     */
    private final String username;

    /**
     * The clientId
     */
    private String clientId;

    public IdConstruct(String partition, String username, String clientId) {
        this.partition = partition;
        this.username = username;
        this.clientId = clientId;
    }


    public String getUsername() {
        return username;
    }

    public String getPartition() {
        return partition;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IdConstruct) {
            IdConstruct sa = (IdConstruct) o;

            if (StringUtils.isEmpty(getClientId()) || StringUtils.isEmpty(sa.getClientId())) {

                return (Objects.equals(sa.getPartition(), getPartition())
                        && Objects.equals(sa.getUsername(), getUsername()));
            } else
                return (
                        Objects.equals(sa.getClientId(), getClientId())
                                && Objects.equals(sa.getPartition(), getPartition())
                                && Objects.equals(sa.getUsername(), getUsername()));

        }
        return false;
    }

}
