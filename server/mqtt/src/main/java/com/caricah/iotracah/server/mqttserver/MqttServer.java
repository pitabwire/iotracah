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

import com.caricah.iotracah.core.modules.Server;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.server.ServerInterface;
import com.caricah.iotracah.server.mqttserver.netty.MqttServerHandler;
import com.caricah.iotracah.server.mqttserver.netty.MqttServerImpl;
import com.caricah.iotracah.server.mqttserver.transform.IOTMqttTransformerImpl;
import com.caricah.iotracah.server.mqttserver.transform.MqttIOTTransformerImpl;
import com.caricah.iotracah.server.transform.IOTMqttTransformer;
import com.caricah.iotracah.server.transform.MqttIOTTransformer;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.apache.commons.configuration.Configuration;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/16/15
 */
public class MqttServer extends Server<MqttMessage> {

    private ServerInterface<MqttMessage> serverImpl;
    private IOTMqttTransformer<MqttMessage> iotMqttTransformer;
    private MqttIOTTransformer<MqttMessage> mqttIOTTransformer;

    /**
     * <code>configure</code> allows the base system to configure itself by getting
     * all the settings it requires and storing them internally. The plugin is only expected to
     * pick the settings it has registered on the configuration file for its particular use.
     *
     * @param configuration
     * @throws UnRetriableException
     */
    @Override
    public void configure(Configuration configuration) throws UnRetriableException {

        log.info(" configure : setting up our configurations.");

        serverImpl = new MqttServerImpl(this);
        serverImpl.configure(configuration);

        iotMqttTransformer = new IOTMqttTransformerImpl();
        mqttIOTTransformer = new MqttIOTTransformerImpl();
    }

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    @Override
    public void initiate() throws UnRetriableException {

        log.info(" configure : initiating the netty server.");
        serverImpl.initiate();
    }

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    @Override
    public void terminate() {

        log.info(" terminate : stopping any processing. ");
        serverImpl.terminate();
    }

    /**
     * Provides the Observer with a new item to observe.
     * <p>
     * The {@link com.caricah.iotracah.core.modules.Worker} may call this method 0 or more times.
     * <p>
     * The {@code Observable} will not call this method again after it calls either {@link #onCompleted} or
     * {@link #onError}.
     *
     * @param ioTMessage the item emitted by the Observable
     */
    @Override
    public void onNext(IOTMessage ioTMessage) {

        if(null == ioTMessage || !Protocal.MQTT.equals(ioTMessage.getProtocal())){
            return;
        }

        log.debug(" MqttServer onNext : message outbound {}", ioTMessage);


        MqttMessage mqttMessage = toServerMessage(ioTMessage);

        if(null == mqttMessage){
            log.debug(" MqttServer onNext : ignoring outbound message {}", ioTMessage);
        }else {
            serverImpl.pushToClient(ioTMessage.getConnectionId(), mqttMessage);
        }
        serverImpl.postProcess(ioTMessage);
    }


    /**
     * Implementation is expected to transform a server specific message
     * to an internal message that the iotracah workers can handle.
     * <p>
     * Everything that goes beyond the server to workers and eventers
     * or the other way round.
     *
     * @param serverMessage
     * @return
     */
    @Override
    protected IOTMessage toIOTMessage(MqttMessage serverMessage) {
       return mqttIOTTransformer.toIOTMessage(serverMessage);
   }

    /**
     * Implementation transforms the internal message to a server specific message
     * that the server now knows how to handle.
     * <p>
     * At the risk of making iotracah create so many unwanted objects,
     * This would be the best way to just ensure the appropriate plugin separation
     * is maintained.
     *
     * @param internalMessage
     * @return
     */
    @Override
    protected MqttMessage toServerMessage(IOTMessage internalMessage) {
        return iotMqttTransformer.toServerMessage(internalMessage);
    }

    /**
     * Declaration by the server implementation if its connections are persistant
     * Or not.
     * Persistent connections are expected to store some control data within the server
     * to ensure successive requests are identifiable.
     *
     * @return
     */
    @Override
    public boolean isPersistentConnection() {
        return true;
    }

    @Override
    public Protocal getProtocal() {
        return Protocal.MQTT;
    }
}
