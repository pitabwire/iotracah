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

package com.caricah.iotracah.datastore.ignitecache.internal.impl;

import com.caricah.iotracah.core.worker.state.models.ClSubscription;
import com.caricah.iotracah.datastore.ignitecache.internal.AbstractHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import rx.Observable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public class SubscriptionHandler extends AbstractHandler<ClSubscription> {

    public static final String CONFIG_IGNITECACHE_SUBSCRIPTION_CACHE_NAME = "config.ignitecache.subscription.cache.name";
    public static final String CONFIG_IGNITECACHE_SUBSCRIPTION_CACHE_NAME_VALUE_DEFAULT = "iotracah_client_subscription_cache";


    @Override
    public void configure(Configuration configuration) {


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_SUBSCRIPTION_CACHE_NAME, CONFIG_IGNITECACHE_SUBSCRIPTION_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

    }


    public Observable<ClSubscription> getSubscription(String partition, Object[] subscriptionFilterKeyList) {

        String query = "SELECT s._val FROM ClSubscription s WHERE  s.partition = ? AND s.topicFilterKey IN (%s)";

        query = String.format(query, preparePlaceHolders(subscriptionFilterKeyList.length));

        Object[] params = {partition};
        Object[] finalParams = Arrays.copyOf(params, params.length + subscriptionFilterKeyList.length);
        System.arraycopy(subscriptionFilterKeyList, 0, finalParams, params.length, subscriptionFilterKeyList.length);

        return getByQueryAsValue(query, finalParams);

//        String query = "SELECT * FROM ClSubscription s, TABLE(id varchar =?) n WHERE  s.partition = ? AND s.topicFilterKey = n.id";
//
//        Object[] params = { subscriptionFilterKeyList, partition};
//        return getByQueryAsValue(query, params);




//        return Observable.create(observer -> {
//
//            try {
//
//                Set<Serializable> mySet = new HashSet<>();
//                Collections.addAll(mySet, subscriptionFilterKeyList);
//
//                // Find only persons earning more than 1,000.
//                try (QueryCursor cursor = getDatastoreCache().query(new ScanQuery<Serializable, ClSubscription>((k, p) -> { return p.getPartition().equals(partition) &&  mySet.contains(p.getTopicFilterKey());}))){
//                    for(Object subscription : cursor) {
//
//                        log.debug(" getSubscription : Obtained the subscription {}", subscription);
//
//                        observer.onNext((ClSubscription) subscription);
//
//                    }
//                }
//
//                log.debug(" getSubscription : Nothing found");
//                log.debug(" getSubscription : Nothing found++++++++++++++++++++++++++++++++++");
//
//
//                observer.onCompleted();
//            } catch (Exception e) {
//                observer.onError(e);
//            }
//
//        });


    }




}
