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

import com.caricah.iotracah.core.worker.state.messages.ConnectMessage;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.modules.Server;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.server.mqttserver.netty.ServerImpl;
import com.caricah.iotracah.server.mqttserver.netty.TimeoutHandler;
import com.caricah.iotracah.server.mqttserver.transform.IOTMqttTransformerImpl;
import com.caricah.iotracah.server.mqttserver.transform.MqttIOTTransformerImpl;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.configuration.Configuration;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/16/15
 */
public class MqttServer extends Server<MqttMessage> {

    private ServerInterface serverImpl;
    private IOTMqttTransformer iotMqttTransformer;
    private MqttIOTTransformer mqttIOTTransformer;

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

        logInfo(" configure : setting up our configurations.");

        serverImpl = new ServerImpl(this);
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

        logInfo(" configure : initiating the netty server.");
        serverImpl.initiate();
    }

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    @Override
    public void terminate() {

        logInfo(" terminate : stopping any processing. ");
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

        logDebug(" MqttServer onNext : message outbound {}", ioTMessage);

        if(null == ioTMessage){
            return;
        }

        MqttMessage mqttMessage = toServerMessage(ioTMessage);
        serverImpl.pushToClient(ioTMessage.getConnectionId(), mqttMessage);

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

    @Override
    public Protocal getProtocal() {
        return Protocal.MQTT;
    }
}
