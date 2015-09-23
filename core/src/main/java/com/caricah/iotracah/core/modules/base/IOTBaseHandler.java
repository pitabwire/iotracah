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

package com.caricah.iotracah.core.modules.base;

import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.core.modules.Eventer;
import com.caricah.iotracah.system.BaseSystemHandler;
import rx.Observable;
import rx.Subscriber;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/3/15
 */
public abstract class IOTBaseHandler extends Subscriber<IOTMessage> implements Observable.OnSubscribe<IOTMessage>, BaseSystemHandler {



    Map<Protocal, Eventer> eventerMap = new HashMap<>();

    private UUID nodeId;
    private String cluster;

    public abstract Protocal getProtocal();



    public UUID getNodeId(){
        return nodeId;
    }
    public void setNodeId(UUID nodeId){
        this.nodeId = nodeId;
    }

    public String getCluster(){
        return cluster;
    }
    public void setCluster(String cluster){
        this.cluster = cluster;
    }

    public void statGauge(String gaugeName, double value){}
    public void statCounterIncrement(String counterName){}
    public void statCounterDecrement(String counterName){}
    public void logDebug(String message, Object ... params){}
    public void logInfo(String message, Object ... params){}
    public void logError(String message, Throwable e){}

    @Override
    public void call(Subscriber<? super IOTMessage> subscriber) {

        if(subscriber instanceof Eventer){
            Eventer eventer = (Eventer) subscriber;
            eventerMap.put(eventer.getProtocal(), eventer);
        }

    }


    /**
     * Notifies the Observer that the {@link Observable} has finished sending push-based notifications.
     * <p>
     * The {@link Observable} will not call this method if it calls {@link #onError}.
     */
    @Override
    public void onCompleted() {

        logError(" onCompleted : Critical internal error due to wrong method calls ", new IllegalStateException("Method not supposed to be called by anyone in the lifetime of iotracah"));
    }

    /**
     * Notifies the Observer that the {@link Observable} has experienced an error condition.
     * <p>
     * If the {@link Observable} calls this method, it will not thereafter call {@link #onNext} or
     * {@link #onCompleted}.
     *
     * @param e the exception encountered by the Observable
     */
    @Override
    public void onError(Throwable e) {
        logError(" onError : Critical internal error due to wrong method calls ", e);
    }


}
