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

package com.caricah.iotracah.server.mqttserver;

import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.exceptions.UnRetriableException;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/23/15
 */
public interface ServerInterface {

    void configure(Configuration configuration) throws UnRetriableException;

    void initiate() throws UnRetriableException;

    void terminate();

    void pushToClient(Serializable connectionId, MqttMessage mqttMessage);

    void postProcess(IOTMessage ioTMessage);
}
