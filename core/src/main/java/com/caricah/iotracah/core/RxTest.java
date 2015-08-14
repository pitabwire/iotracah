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

package com.caricah.iotracah.core;


import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/12/15
 */
public class RxTest {

    List<Subscriber<? super String>> subscribers = new ArrayList<>();

    public static void main(String[] args) {

        RxTest rxTest = new RxTest();
        rxTest.startOp();
    }

    public void genEvent(String data) {
        for (Subscriber s: subscribers)
            s.onNext(data);
    }

    private void startOp() {

        Observable<String> myObservable = Observable.create(new RxObserverble());
        Subscriber<String> mySubscriber = new RxSubscriber();

        myObservable.subscribeOn(Schedulers.computation()).subscribe(mySubscriber);

                new NumberGenerator(this).start();
    }


    private class RxObserverble implements Observable.OnSubscribe<String> {

        @Override
        public void call(Subscriber<? super String> subscriber) {
            subscribers.add(subscriber);
        }
    }

    private class RxSubscriber extends Subscriber<String>{

        @Override
        public void onCompleted() {

        }

        @Override
        public void onError(Throwable e) {

        }

        @Override
        public void onNext(String s) {
            System.out.println("Subscriber mentality : "+ s);
        }
    }


}
