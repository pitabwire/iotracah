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
import com.caricah.iotracah.system.BaseSystemHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/3/15
 */
public abstract class IOTBaseHandler extends Subscriber<IOTMessage> implements Observable.OnSubscribe<IOTMessage>, BaseSystemHandler {


    protected final Logger log = LoggerFactory.getLogger(getClass());

    private List<Subscriber> subscriberList = new ArrayList<>();

    private UUID nodeId;
    private String cluster;

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

    public List<Subscriber> getSubscriberList() {
        return subscriberList;
    }

    public void setSubscriberList(List<Subscriber> subscriberList) {
        this.subscriberList = subscriberList;
    }

    public void statGauge(String gaugeName, double value){}
    public void statCounterIncrement(String counterName){}
    public void statCounterDecrement(String counterName){}

    @Override
    public void call(Subscriber<? super IOTMessage> subscriber) {

        log.info(" call : there was a subscription by {} to {} for updates", subscriber, this);

        subscriberList.add(subscriber);
    }


    /**
     * Notifies the Observer that the {@link Observable} has finished sending push-based notifications.
     * <p>
     * The {@link Observable} will not call this method if it calls {@link #onError}.
     */
    @Override
    public void onCompleted() {

        log.error(" onCompleted : Critical internal error due to wrong method calls ", new IllegalStateException("Method not supposed to be called by anyone in the lifetime of iotracah"));
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
        log.error(" onError : Critical internal error due to wrong method calls ", e);
    }


}
