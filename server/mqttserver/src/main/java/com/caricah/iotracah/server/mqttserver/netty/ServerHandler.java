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

import com.caricah.iotracah.core.modules.Server;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.mqtt.MqttMessage;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/27/15
 */
public class ServerHandler extends SimpleChannelInboundHandler<MqttMessage> {

    private final ServerImpl serverImpl;

    public ServerHandler(ServerImpl serverImpl) {

        this.serverImpl = serverImpl;
    }

    public ServerImpl getServerImpl() {
        return serverImpl;
    }

    public Server<MqttMessage> getInternalServer() {
        return getServerImpl().getInternalServer();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ChannelGroup channelGroup = getServerImpl().getChannelGroup();
        channelGroup.add(channel);
        super.channelActive(ctx);
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        String clientId = ctx.channel().attr(ServerImpl.REQUEST_CLIENT_ID).get();

        if (null != clientId) {

            Serializable connectionId = ctx.channel().attr(ServerImpl.REQUEST_CONNECTION_ID).get();
            Serializable sessionId = ctx.channel().attr(ServerImpl.REQUEST_SESSION_ID).get();

            getInternalServer().dirtyDisconnect(connectionId, sessionId, clientId);

        }
    }

    /**
     * Is called for each message of type {@link MqttMessage}.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
     *            belongs to
     * @param msg the message to handle
     * @throws Exception is thrown if an error occurred
     */
    @Override
    protected void messageReceived(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {

        getInternalServer().logDebug(" messageReceived : received the message {}", msg);

        String clientId = ctx.channel().attr(ServerImpl.REQUEST_CLIENT_ID).get();
        Serializable connectionId = ctx.channel().attr(ServerImpl.REQUEST_CONNECTION_ID).get();
        Serializable sessionId = ctx.channel().attr(ServerImpl.REQUEST_SESSION_ID).get();

        getInternalServer().pushToWorker(connectionId, sessionId, clientId, msg);

    }


}
