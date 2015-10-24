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

import com.caricah.iotracah.core.worker.state.Constant;
import com.caricah.iotracah.core.worker.state.models.SubscriptionFilter;
import com.caricah.iotracah.datastore.ignitecache.internal.AbstractHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.configuration.CacheConfiguration;
import rx.Observable;
import rx.Subscriber;

import javax.cache.Cache;
import java.io.Serializable;
import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public class SubscriptionFilterHandler extends AbstractHandler<SubscriptionFilter> {

    public static final String CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME = "config.ignitecache.subscription.filter.cache.name";
    public static final String CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME_VALUE_DEFAULT = "iotracah_subscription_filter_cache";


    @Override
    public void configure(Configuration configuration) {


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME, CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

    }

    @Override
    protected CacheConfiguration setIndexData(Class<SubscriptionFilter> t, CacheConfiguration clCfg) {
        return clCfg.setIndexedTypes(Long.class, t);
    }


    public Observable<SubscriptionFilter> matchTopicFilterTree(String partition, List<String> topicNavigationRoute) {


        return Observable.create(observer -> {

            List<Long> collectingParentIdList = new ArrayList<>();
            collectingParentIdList.add(0l);

            ListIterator<String> pathIterator = topicNavigationRoute.listIterator();

            try {

                while (pathIterator.hasNext()) {

                    String name = pathIterator.next();

                    List<Long> parentIdList = new ArrayList<>(collectingParentIdList);
                    collectingParentIdList.clear();

                    for (Long parentId : parentIdList) {


                            String  query = "partition = ? AND parentId = ? AND name IN (?, ?, ?) ";
                            Object[]  params = new Object[]{partition, parentId, name, Constant.MULTI_LEVEL_WILDCARD, Constant.SINGLE_LEVEL_WILDCARD};

                            getByQuery(SubscriptionFilter.class, query, params)
                                    .toBlocking().forEach(subscriptionFilter -> {

                                if (pathIterator.hasNext()) {

                                    if (Constant.MULTI_LEVEL_WILDCARD.equals(subscriptionFilter.getName())) {
                                        observer.onNext(subscriptionFilter);
                                    } else {
                                        collectingParentIdList.add(subscriptionFilter.getId());
                                    }


                                } else {
                                    observer.onNext(subscriptionFilter);
                                }

                            });

                        }
                    }



                observer.onCompleted();


            } catch (Exception e) {
                observer.onError(e);
            }

        });



    }


    public Observable<SubscriptionFilter> getTopicFilterTree(String partition, List<String> topicFilterTreeRoute) {

        return Observable.create(observer -> {

            List<Long> collectingParentIdList = new ArrayList<>();
            collectingParentIdList.add(0l);

            ListIterator<String> pathIterator = topicFilterTreeRoute.listIterator();

            try {

                while (pathIterator.hasNext()) {

                    String name = pathIterator.next();

                    List<Long> parentIdList = new ArrayList<>(collectingParentIdList);
                    collectingParentIdList.clear();

                    for (Long parentId : parentIdList) {

                      if( Constant.MULTI_LEVEL_WILDCARD.equals(name)) {

                          getMultiLevelWildCard(observer, partition, parentId);
                      }else if (Constant.SINGLE_LEVEL_WILDCARD.equals(name)) {

                            String query = "partition = ? AND parentId = ? ";
                            Object[] params = {partition, parentId};

                            getByQuery(SubscriptionFilter.class, query, params)
                                    .toBlocking().forEach(subscriptionFilter -> {

                                if (pathIterator.hasNext()) {
                                    collectingParentIdList.add(subscriptionFilter.getId());
                                } else {
                                    observer.onNext(subscriptionFilter);
                                }

                            });

                        }else{




                          String  query = "partition = ? AND parentId = ? AND name = ? ";
                            Object[]  params = new Object[]{partition, parentId, name};

                                getByQuery(SubscriptionFilter.class, query, params)
                                        .toBlocking().forEach(subscriptionFilter -> {

                                    if (pathIterator.hasNext()) {
                                        collectingParentIdList.add(subscriptionFilter.getId());
                                    } else {
                                        observer.onNext(subscriptionFilter);
                                    }

                                });

                        }
                    }

                }

                observer.onCompleted();


            } catch (Exception e) {
                observer.onError(e);
            }

        });

    }

    private void getMultiLevelWildCard(Subscriber<? super SubscriptionFilter> observer, String partition, Long parentId) {

        String query = "partition = ? AND parentId = ? ";
        Object[] params = {partition, parentId};

        getByQuery(SubscriptionFilter.class, query, params)
                .toBlocking().forEach(subscriptionFilter -> {

            observer.onNext(subscriptionFilter);

            getMultiLevelWildCard(observer, partition, subscriptionFilter.getId());

        });


    }

    public Observable<SubscriptionFilter> createTree(String partition, List<String> topicFilterTreeRoute) {


        return Observable.create(observer -> {

                    try {
                        String currentTreeName = "";
                        SubscriptionFilter activeSubscriptionFilter = null;

                        ListIterator<String> pathIterator = topicFilterTreeRoute.listIterator();
                        long parentId = 0l;

                        while (pathIterator.hasNext()) {

                            String name;
                            if (!pathIterator.hasPrevious()) {
                                name = pathIterator.next();
                                currentTreeName = name;
                            } else {
                                name = pathIterator.next();
                                currentTreeName += Constant.PATH_SEPARATOR + name;
                            }


                            String query = "partition = ? AND parentId = ? AND name = ? ";
                            Object[] params = {partition, parentId, name};

                            SubscriptionFilter internalSubscriptionFilter = getByQuery(SubscriptionFilter.class, query, params).toBlocking().singleOrDefault(null);

                            if (null == internalSubscriptionFilter) {
                                internalSubscriptionFilter = new SubscriptionFilter();
                                internalSubscriptionFilter.setPartition(partition);
                                internalSubscriptionFilter.setParentId(parentId);
                                internalSubscriptionFilter.setFullTreeName(currentTreeName);
                                internalSubscriptionFilter.setName(name);
                                internalSubscriptionFilter.setId(nextId());
                                save(internalSubscriptionFilter);

                            }


                            if (!pathIterator.hasNext()) {
                                activeSubscriptionFilter = internalSubscriptionFilter;
                            } else {
                                parentId = internalSubscriptionFilter.getId();
                            }
                        }

                        if (null != activeSubscriptionFilter) {
                            observer.onNext(activeSubscriptionFilter);
                        }
                        observer.onCompleted();
                    } catch (Exception e) {
                        observer.onError(e);
                    }

                }
        );
    }

}
