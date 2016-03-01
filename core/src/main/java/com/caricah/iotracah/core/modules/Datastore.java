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

package com.caricah.iotracah.core.modules;

import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.data.models.messages.IotMessageKey;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilter;
import com.caricah.iotracah.bootstrap.data.models.subscriptions.IotSubscription;
import com.caricah.iotracah.bootstrap.security.realm.IOTSecurityDatastore;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import com.caricah.iotracah.bootstrap.system.BaseSystemHandler;
import com.caricah.iotracah.core.worker.exceptions.DoesNotExistException;
import com.caricah.iotracah.core.worker.state.Constant;
import org.apache.ignite.Ignite;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.session.mgt.DefaultSessionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;

import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 *
 * <code>Datastore</code> provides the abstraction required to hide
 * all the data access.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/10/15
 */
public abstract class Datastore implements IOTSecurityDatastore, Observable.OnSubscribe<IOTMessage>,BaseSystemHandler {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    List<Eventer> eventerList = new ArrayList<>();

    private Ignite ignite;

    private ExecutorService executorService;

    protected Ignite getIgnite() {
        return ignite;
    }

    public void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public Observable<IOTClient> getSession(IotClientKey sessionId){

        return Observable.create(   observer ->{

                 try{

                     DefaultSessionKey sessionKey = new DefaultSessionKey(sessionId);

                    IOTClient session = (IOTClient) SecurityUtils.getSecurityManager().getSession(sessionKey);
                    if(Objects.isNull(session)){
                        observer.onError(new DoesNotExistException("No session with the id exists."));
                    }else {
                        observer.onNext(session);
                        observer.onCompleted();
                    }

                 }catch (Exception e){
                     observer.onError(e);
                 }

                }
        );
    }

    public abstract Observable<PublishMessage> getWill(IOTClient client);
    public abstract void saveWill(IOTClient iotSession, PublishMessage publishMessage);
    public abstract void removeWill(IOTClient client);

    public abstract Observable<IotSubscriptionFilter> getMatchingSubscriptionFilter(String partition, String topic);
    public abstract Observable<IotSubscriptionFilter> getOrCreateSubscriptionFilter(String partition, String topic);
    public abstract Observable<IotSubscriptionFilter> getSubscriptionFilterTree(String partitionId, String topic);

    public abstract void removeSubscriptionFilter(IotSubscriptionFilter subscriptionFilter);


    public abstract Observable<IotSubscription> getSubscriptions(IOTClient client);
    public abstract Observable<IotSubscription> getSubscriptions( IotSubscriptionFilter iotSubscriptionFilter, int qos);
    public abstract Observable<IotSubscription> getSubscriptionWithDefault(IotSubscription subscription);
    public abstract void saveSubscription(IotSubscription subscription);
    public abstract void removeSubscription(IotSubscription subscription);




    public abstract Observable<PublishMessage> getMessages(IOTClient session);
    public abstract Observable<PublishMessage> getMessage(IOTClient iotClient, long messageId, boolean isInbound) ;

    public abstract Observable<Map.Entry<Long, IotMessageKey>> saveMessage(PublishMessage publishMessage);

    public abstract void removeMessage(PublishMessage publishMessage);

    public abstract Observable<PublishMessage> getRetainedMessage(IotSubscriptionFilter subscriptionFilter) ;

    public abstract void saveRetainedMessage(IotSubscriptionFilter subscriptionFilter, int qos, Object payload);

    public abstract void removeRetainedMessage(IotSubscriptionFilter subscriptionFilter);

    /**
     * getTopicNavigationRoute is a utility method to breakdown route to
     * Subscription filter names...
     *
     * @param topicFilter
     * @return
     */
    public List<String> getTopicNavigationRoute(String topicFilter) {

        List<String> topicBreakDownSet = new LinkedList<>();

        String[] topicLevels = topicFilter.split(Constant.PATH_SEPARATOR);

        Collections.addAll(topicBreakDownSet, topicLevels);

        return topicBreakDownSet;
    }


    @Override
    public void call(Subscriber<? super IOTMessage> subscriber) {

        if(subscriber instanceof Eventer){
            eventerList.add((Eventer) subscriber);
        }

    }

    @Override
    public int compareTo(BaseSystemHandler baseSystemHandler) {

        if(null == baseSystemHandler)
            throw new NullPointerException("You can't compare a null object.");

        if(baseSystemHandler instanceof Datastore)
            return 0;
        else if(baseSystemHandler instanceof Eventer)
            return -1;

        else
            return 1;
    }


  }
