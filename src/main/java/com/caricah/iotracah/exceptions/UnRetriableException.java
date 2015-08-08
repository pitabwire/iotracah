/*
 * Copyright (c) 2015. Caricah Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.caricah.iotracah.exceptions;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/26/15
 */
public class UnRetriableException extends Exception {

    public UnRetriableException() {
    }

    public UnRetriableException(String message) {
        super(message);
    }

    public UnRetriableException(String message, Throwable var2) {
        super(message, var2);
    }

    public UnRetriableException(Throwable message) {
        super(message);
    }

   

}
