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

package com.caricah.iotracah.server.netty;

import com.caricah.iotracah.core.modules.Server;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.handler.codec.mqtt.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/27/15
 */
public abstract class ServerHandler<T> extends SimpleChannelInboundHandler<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final ServerImpl<T> serverImpl;

    public ServerHandler(ServerImpl<T> serverImpl) {
        this.serverImpl = serverImpl;
    }

    public ServerImpl<T> getServerImpl() {
        return serverImpl;
    }

    public Server<T> getInternalServer() {
        return getServerImpl().getInternalServer();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        ChannelGroup channelGroup = getServerImpl().getChannelGroup();

        ctx.channel().attr(ServerImpl.REQUEST_CONNECTION_ID).set(channel.id().asLongText());

        channelGroup.add(channel);
        super.channelActive(ctx);
    }


    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

        String sessionId = ctx.channel().attr(ServerImpl.REQUEST_SESSION_ID).get();

        if (null != sessionId) {

            String connectionId = ctx.channel().attr(ServerImpl.REQUEST_CONNECTION_ID).get();

            getInternalServer().dirtyDisconnect(connectionId, sessionId);

        }
    }


    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }


}
