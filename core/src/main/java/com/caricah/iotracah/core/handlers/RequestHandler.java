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

package com.caricah.iotracah.core.handlers;


import com.caricah.iotracah.core.modules.Datastore;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.Messenger;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.StringJoiner;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public abstract class RequestHandler {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private Worker worker;

    public Worker getWorker() {
        return worker;
    }

    public void setWorker(Worker worker) {
        this.worker = worker;
    }

    public Datastore getDatastore(){
        return getWorker().getDatastore();
    }

    public Messenger getMessenger(){
        return  getWorker().getMessenger();
    }

    public Observable<Client> getClient(String partition, String clientIdentifier) {
        return getDatastore().getClient(partition, clientIdentifier);
    }

    private String getPermission(AuthorityRole role, String topic) {
        return new StringJoiner("").add(role.name()).add(":").add(topic).toString();
    }

    public void checkPermission(AuthorityRole role, String topic) {

        String permission = getPermission(role, topic);
    }


    public void disconnectDueToError(Exception e){

    }
    /**
     * Wrapper method to assist in pushing/routing requests to the client.
     * It provides convenience to access the active worker and push
     * out the available messages to the connected client.
     * @param iotMessage
     */
    public void pushToServer(IOTMessage iotMessage){


        getWorker().pushToServer(iotMessage);

    }

    public abstract void handle() throws RetriableException, UnRetriableException;
}
