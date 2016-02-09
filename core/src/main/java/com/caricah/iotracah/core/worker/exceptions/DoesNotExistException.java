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

package com.caricah.iotracah.core.worker.exceptions;

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/2/15
 */
public class DoesNotExistException extends UnRetriableException {

    /**
     * Creates a new instance.
     */
    public DoesNotExistException() {
    }

    /**
     * Creates a new instance.
     */
    public DoesNotExistException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance.
     */
    public DoesNotExistException(String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     */
    public DoesNotExistException(Throwable cause) {
        super(cause);
    }
}
