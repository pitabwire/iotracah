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

package com.caricah.iotracah.bootstrap.data.messages.base;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/3/15
 */
public enum Protocol implements Serializable{

    MQTT(true), HTTP(false);

    private final boolean persistent;

    Protocol(boolean persistent){
        this.persistent = persistent;
    }

    public boolean isPersistent(){
        return persistent;
    }

    public boolean isNotPersistent(){
        return !persistent;
    }


    @Override
    public String toString() {
        return this.name();
    }

    public static Protocol fromString(String name){
        switch (name){

            case "MQTT":
                return MQTT;
            case "HTTP":
                return HTTP;
        }
        throw new IllegalArgumentException("The protocal specified is not implemented. Can you work on it?");
    }
}
