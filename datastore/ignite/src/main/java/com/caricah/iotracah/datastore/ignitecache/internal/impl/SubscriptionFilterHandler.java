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

import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.CacheConfig;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilter;
import com.caricah.iotracah.bootstrap.data.models.subscriptionfilters.IotSubscriptionFilterKey;
import com.caricah.iotracah.core.worker.state.Constant;
import com.caricah.iotracah.datastore.ignitecache.internal.AbstractHandler;
import org.apache.commons.configuration.Configuration;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.configuration.CacheConfiguration;
import rx.Observable;
import rx.Subscriber;

import javax.sql.DataSource;
import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/20/15
 */
public class SubscriptionFilterHandler extends AbstractHandler<IotSubscriptionFilterKey, IotSubscriptionFilter> {

    public static final String CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME = "config.ignitecache.subscription.filter.cache.name";
    public static final String CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME_VALUE_DEFAULT = "iotracah_subscription_filter_cache";


    @Override
    public void configure(Configuration configuration) {


        String cacheName = configuration.getString(CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME, CONFIG_IGNITECACHE_SUBSCRIPTION_FILTER_CACHE_NAME_VALUE_DEFAULT);
        setCacheName(cacheName);

    }

    @Override
    protected CacheConfiguration<IotSubscriptionFilterKey, IotSubscriptionFilter> getCacheConfiguration(boolean persistanceEnabled,  DataSource ds) {
        CacheJdbcPojoStoreFactory<IotSubscriptionFilterKey, IotSubscriptionFilter> factory = null;

        if (persistanceEnabled){
            factory = new CacheJdbcPojoStoreFactory<>();
            factory.setDataSource(ds);
        }


        return CacheConfig.cache(getCacheName(), factory);
    }


    @Override
    public IotSubscriptionFilterKey keyFromModel(IotSubscriptionFilter model) {

        return keyFromTopic(model.getPartitionId(), model.getName());
    }

    public IotSubscriptionFilterKey keyFromList(String partitionId, List<String> topicList) {

        IotSubscriptionFilterKey filterKey = new IotSubscriptionFilterKey();
        filterKey.setName(String.join(Constant.PATH_SEPARATOR, topicList));
        filterKey.setPartitionId(partitionId);
        return filterKey;
    }

    public IotSubscriptionFilterKey keyFromTopic(String partitionId, String topicName) {

        IotSubscriptionFilterKey filterKey = new IotSubscriptionFilterKey();
        filterKey.setName(topicName);
        filterKey.setPartitionId(partitionId);
        return filterKey;
    }

    public Observable<IotSubscriptionFilter> matchTopicFilterTree(String partitionId, List<String> topicNavigationRoute) {

        return Observable.create(observer -> {

            Set<IotSubscriptionFilterKey> topicFilterKeys = new HashSet<>();

            ListIterator<String> pathIterator = topicNavigationRoute.listIterator();

            List<String> growingTitles = new ArrayList<>();

            while (pathIterator.hasNext()) {

                String name = pathIterator.next();

                List<String> slWildCardList = new ArrayList<>(growingTitles);

                if (pathIterator.hasNext()) {
                    //We deal with wildcard.
                    slWildCardList.add(Constant.MULTI_LEVEL_WILDCARD);
                    topicFilterKeys.add(keyFromList(partitionId, slWildCardList));

                } else {


                    slWildCardList.add(Constant.SINGLE_LEVEL_WILDCARD);
                    topicFilterKeys.add(keyFromList(partitionId, slWildCardList));

                    slWildCardList.add(Constant.SINGLE_LEVEL_WILDCARD);
                    topicFilterKeys.add(keyFromList(partitionId, slWildCardList));

                    slWildCardList.remove(slWildCardList.size() - 1);
                    slWildCardList.remove(slWildCardList.size() - 1);

                    //we deal with full topic
                    slWildCardList.add(name);
                }

                List<String> reverseSlWildCardList = new ArrayList<>(slWildCardList);

                growingTitles.add(name);

                int sizeOfTopic = slWildCardList.size() - 1;


                for (int i = 0; i <= sizeOfTopic; i++) {

                    if (i < sizeOfTopic) {
                        int reverseIndex = sizeOfTopic - i;

                        slWildCardList.set(i, Constant.SINGLE_LEVEL_WILDCARD);
                        reverseSlWildCardList.set(reverseIndex, Constant.SINGLE_LEVEL_WILDCARD);

                        topicFilterKeys.add(keyFromList(partitionId, slWildCardList));
                        topicFilterKeys.add(keyFromList(partitionId, reverseSlWildCardList));

                    } else {

                        if (!pathIterator.hasNext()) {

                            slWildCardList.set(i, Constant.SINGLE_LEVEL_WILDCARD);
                            topicFilterKeys.add(keyFromList(partitionId, slWildCardList));

                            slWildCardList.add(Constant.SINGLE_LEVEL_WILDCARD);
                            topicFilterKeys.add(keyFromList(partitionId, slWildCardList));

                            slWildCardList.set(slWildCardList.size() - 1, Constant.MULTI_LEVEL_WILDCARD);
                            topicFilterKeys.add(keyFromList(partitionId, slWildCardList));

                        }

                    }
                }

            }

            topicFilterKeys.add(keyFromList(partitionId, growingTitles));

            growingTitles.add(Constant.SINGLE_LEVEL_WILDCARD);
            topicFilterKeys.add(keyFromList(partitionId, growingTitles));

            growingTitles.set(growingTitles.size() - 1, Constant.MULTI_LEVEL_WILDCARD);
            topicFilterKeys.add(keyFromList(partitionId, growingTitles));

            growingTitles.remove(growingTitles.size() - 1);

            growingTitles.set(growingTitles.size() - 1, Constant.SINGLE_LEVEL_WILDCARD);
            topicFilterKeys.add(keyFromList(partitionId, growingTitles));

            growingTitles.set(growingTitles.size() - 1, Constant.MULTI_LEVEL_WILDCARD);
            topicFilterKeys.add(keyFromList(partitionId, growingTitles));


            //Add lost wildcard
            getBySet(topicFilterKeys).subscribe(observer::onNext, observer::onError, observer::onCompleted);

        });


    }


    public Observable<IotSubscriptionFilter> getTopicFilterTree(String partition, List<String> topicFilterTreeRoute) {

        return Observable.create(observer -> {

            List<Long> collectingParentIdList = new ArrayList<>();

            collectingParentIdList.add(0l);

            List<String> growingTitles = new ArrayList<>();

            ListIterator<String> pathIterator = topicFilterTreeRoute.listIterator();

            try {

                while (pathIterator.hasNext()) {


                    String topicPart = pathIterator.next();

                    log.debug(" getTopicFilterTree : current path in tree is : {}", topicPart);

                    growingTitles.add(topicPart);

                    List<Long> parentIdList = new ArrayList<>(collectingParentIdList);
                    collectingParentIdList.clear();

                    for (Long parentId : parentIdList) {

                        log.debug(" getTopicFilterTree : Dealing with parent id : {} and titles is {}", parentId, growingTitles);

                        if (Constant.MULTI_LEVEL_WILDCARD.equals(topicPart)) {

                            getMultiLevelWildCard(observer, partition, parentId);
                        } else if (Constant.SINGLE_LEVEL_WILDCARD.equals(topicPart)) {

                            String query = "partitionId = ? AND parentId = ? ";
                            Object[] params = {partition, parentId};

                            getByQuery(IotSubscriptionFilter.class, query, params)
                                    .toBlocking().forEach(subscriptionFilter -> {

                                log.debug(" getTopicFilterTree : Found matching single level filter : {}", subscriptionFilter);

                                if (pathIterator.hasNext()) {
                                    collectingParentIdList.add(subscriptionFilter.getId());

                                } else {
                                    observer.onNext(subscriptionFilter);
                                }

                            });

                        } else {


                            String query = "partitionId = ? AND parentId = ? AND name = ? ";


                            String joinedTopicName = String.join(Constant.PATH_SEPARATOR, growingTitles);

                            Object[] params = new Object[]{partition, parentId, joinedTopicName};

                            getByQuery(IotSubscriptionFilter.class, query, params)
                                    .toBlocking().forEach(subscriptionFilter -> {

                                log.debug(" getTopicFilterTree : Found matching point filter : {}", subscriptionFilter);


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


    private void getMultiLevelWildCard(Subscriber<? super IotSubscriptionFilter> observer, String partition, Long parentId) {

        String query = "partitionId = ? AND parentId = ? ";
        Object[] params = {partition, parentId};

        getByQuery(IotSubscriptionFilter.class, query, params)
                .toBlocking().forEach(subscriptionFilter -> {

            log.debug(" getMultiLevelWildCard : found a matching filter for multilevel : {}", subscriptionFilter);
            observer.onNext(subscriptionFilter);

            getMultiLevelWildCard(observer, partition, subscriptionFilter.getId());


        });


    }


    public Observable<IotSubscriptionFilter> createTree(String partitionId, List<String> topicFilterTreeRoute) {

        return Observable.create(observer -> {

                    try {

                        List<String> growingTitles = new ArrayList<>();
                        LinkedList<Long> growingParentIds = new LinkedList<>();

                        ListIterator<String> pathIterator = topicFilterTreeRoute.listIterator();


                        while (pathIterator.hasNext()) {


                            growingTitles.add(pathIterator.next());

                            IotSubscriptionFilterKey iotSubscriptionFilterKey = keyFromList(partitionId, growingTitles);
                            Observable<IotSubscriptionFilter> filterObservable = getByKeyWithDefault(iotSubscriptionFilterKey, null);

                            filterObservable.subscribe(
                                    internalSubscriptionFilter -> {

                                        if (null == internalSubscriptionFilter) {
                                            internalSubscriptionFilter = new IotSubscriptionFilter();
                                            internalSubscriptionFilter.setPartitionId(partitionId);
                                            internalSubscriptionFilter.setName(iotSubscriptionFilterKey.getName());
                                            internalSubscriptionFilter.setId(getIdSequence().incrementAndGet());

                                            if (growingParentIds.isEmpty()) {
                                                internalSubscriptionFilter.setParentId(0l);
                                            } else {
                                                internalSubscriptionFilter.setParentId(growingParentIds.getLast());
                                            }
                                            save(iotSubscriptionFilterKey, internalSubscriptionFilter);
                                        }

                                        growingParentIds.add(internalSubscriptionFilter.getId());

                                        if (growingTitles.size() == topicFilterTreeRoute.size())
                                            observer.onNext(internalSubscriptionFilter);

                                    }, throwable -> {
                                    }, () -> {


                                        if (!pathIterator.hasNext()) {

                                            observer.onCompleted();

                                        }

                                    });
                        }


                    } catch (Exception e) {
                        observer.onError(e);
                    }

                }
        );
    }

}
