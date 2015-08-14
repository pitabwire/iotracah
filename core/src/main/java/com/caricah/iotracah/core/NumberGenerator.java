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

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/12/15
 */
public class NumberGenerator extends Thread {
    private final RxTest rx;

    public NumberGenerator(RxTest rx) {
            this.rx = rx;
        }

        public void run() {
            for (int i = 0; i < 10; i++) {
                rx.genEvent("Time is "+System.currentTimeMillis());
                try {
                    sleep((int) (Math.random() * 2000));
                } catch (InterruptedException e) {
                }
            }
            System.out.println("Test Finished for: " + getName());
        }

}
