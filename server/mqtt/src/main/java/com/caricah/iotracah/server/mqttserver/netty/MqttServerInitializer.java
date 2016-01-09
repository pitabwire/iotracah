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

package com.caricah.iotracah.server.mqttserver.netty;

import com.caricah.iotracah.server.netty.SSLHandler;
import com.caricah.iotracah.server.netty.ServerHandler;
import com.caricah.iotracah.server.netty.ServerImpl;
import com.caricah.iotracah.server.netty.ServerInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/28/15
 */
public class MqttServerInitializer extends ServerInitializer<MqttMessage> {


    public MqttServerInitializer(ServerImpl<MqttMessage> serverImpl, int connectionTimeout) {
        super(serverImpl, connectionTimeout);
    }

    public MqttServerInitializer(ServerImpl<MqttMessage> serverImpl, int connectionTimeout, SSLHandler sslHandler) {
        super(serverImpl, connectionTimeout, sslHandler);
    }

    @Override
    protected void customizePipeline(EventExecutorGroup eventExecutorGroup, ChannelPipeline pipeline) {
        pipeline.addLast("decoder", new MqttDecoder());
        pipeline.addLast("encoder", new MqttEncoder());

        // we finally have the chance to add some business logic.
        pipeline.addLast(eventExecutorGroup, "iotracah-mqtt",  new MqttServerHandler((MqttServerImpl) getServerImpl()));
    }


}
