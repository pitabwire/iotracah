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
import com.caricah.iotracah.bootstrap.data.messages.DisconnectMessage;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.server.ServerInterface;
import com.caricah.iotracah.server.netty.channelgroup.IotChannelGroup;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/26/15
 */
public abstract class ServerImpl<T> implements ServerInterface<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Server<T> internalServer;

    public static final AttributeKey<String> REQUEST_SESSION_ID = AttributeKey.valueOf("requestSessionIdKey");
    public static final AttributeKey<String> REQUEST_CONNECTION_ID = AttributeKey.valueOf("requestConnectionIdKey");


    private int tcpPort;
    private int sslPort;
    private boolean sslEnabled;
    private int connectionTimeout;

    private SSLHandler sslHandler = null;

    private EventLoopGroup bossEventLoopGroup = null;
    private EventLoopGroup workerEventLoopGroup = null;

    private Channel tcpChannel = null;
    private Channel sslChannel = null;

    private final IotChannelGroup channelGroup = new IotChannelGroup(GlobalEventExecutor.INSTANCE);

    public IotChannelGroup getChannelGroup() {
        return channelGroup;
    }


    public int getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    public int getSslPort() {
        return sslPort;
    }

    public void setSslPort(int sslPort) {
        this.sslPort = sslPort;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public SSLHandler getSslHandler() {
        return sslHandler;
    }

    public void setSslHandler(SSLHandler sslHandler) {
        this.sslHandler = sslHandler;
    }

    public ExecutorService getExecutorService(){
       return getInternalServer().getExecutorService();
    }

    public ServerImpl(Server<T> internalServer) {

        this.internalServer = internalServer;
    }

    public Server<T> getInternalServer() {
        return internalServer;
    }


    /**
     * The @link configure method is responsible for starting the implementation server processes.
     * The implementation should return once the server has started this allows
     * the launcher to maintain the life of the application.
     *
     * @throws UnRetriableException
     */
    public void initiate() throws UnRetriableException {

        log.info(" configure : initiating the netty server.");

        try {


            int countOfAvailableProcessors = Runtime.getRuntime().availableProcessors()+1;

            if (Epoll.isAvailable()) {
                bossEventLoopGroup = new EpollEventLoopGroup(2, getExecutorService());
                workerEventLoopGroup = new EpollEventLoopGroup(countOfAvailableProcessors, getExecutorService());

            } else {
                bossEventLoopGroup = new NioEventLoopGroup(2, getExecutorService());
                workerEventLoopGroup = new NioEventLoopGroup(countOfAvailableProcessors, getExecutorService());
            }


            //Initialize listener for TCP
            ServerBootstrap tcpBootstrap = new ServerBootstrap();
            tcpBootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            tcpBootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
            tcpBootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);

            tcpBootstrap = tcpBootstrap.group(bossEventLoopGroup, workerEventLoopGroup);

            if (Epoll.isAvailable()) {
                tcpBootstrap = tcpBootstrap.channel(EpollServerSocketChannel.class);
            } else {
                tcpBootstrap = tcpBootstrap.channel(NioServerSocketChannel.class);
            }

            tcpBootstrap = tcpBootstrap.handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(getServerInitializer(this, getConnectionTimeout()));

            ChannelFuture tcpChannelFuture = tcpBootstrap.bind(getTcpPort()).sync();
            tcpChannel = tcpChannelFuture.channel();


            if (isSslEnabled()) {
                //Initialize listener for SSL
                ServerBootstrap sslBootstrap = new ServerBootstrap();
                sslBootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
                sslBootstrap.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024);
                sslBootstrap.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);


                sslBootstrap = sslBootstrap.group(bossEventLoopGroup, workerEventLoopGroup);

                if (Epoll.isAvailable()) {
                    sslBootstrap = sslBootstrap.channel(EpollServerSocketChannel.class);
                } else {
                    sslBootstrap = sslBootstrap.channel(NioServerSocketChannel.class);
                }



                sslBootstrap = sslBootstrap.handler(new LoggingHandler(LogLevel.INFO))
                        .childHandler(getServerInitializer(this, getConnectionTimeout(), getSslHandler()));

                ChannelFuture sslChannelFuture = sslBootstrap.bind(getSslPort()).sync();
                sslChannel = sslChannelFuture.channel();
            }

        } catch (InterruptedException e) {

            log.error(" configure : Initialization issues ", e);

            throw new UnRetriableException(e);

        }


    }


    /**
     * @link terminate method is expected to cleanly shut down the server implementation and return immediately.
     */
    public void terminate() {
        log.info(" terminate : stopping any processing. ");

        //Stop all connections.
        getChannelGroup().close().awaitUninterruptibly();


        if (null != sslChannel) {
            sslChannel.close().awaitUninterruptibly();
        }

        if (null != tcpChannel) {
            tcpChannel.close().awaitUninterruptibly();
        }

        if (null != bossEventLoopGroup) {
            bossEventLoopGroup.shutdownGracefully();
        }

        if (null != workerEventLoopGroup) {
            workerEventLoopGroup.shutdownGracefully();
        }


    }


    public void pushToClient(String connectionId, T message) {


        log.debug(" pushToClient : Server pushToClient : we got to now sending out {}", message);

        Channel channel = getChannel(connectionId);

        if (null != channel) {

            //, channel.voidPromise()
            channel.eventLoop().execute(() -> channel.write(message));

        } else {
            log.info(" pushToClient : channel to push message {} is not available ", message);
        }

    }


    public void closeClient(String channelId) {


        Channel channel = getChannel(channelId);
        if (null != channel) {

            channel.attr(ServerImpl.REQUEST_SESSION_ID).set(null);
            channel.attr(ServerImpl.REQUEST_CONNECTION_ID).set(null);

            channel.close();
        }
    }


    @Override
    public void postProcess(IOTMessage ioTMessage) {

        if (DisconnectMessage.MESSAGE_TYPE.equals(ioTMessage.getMessageType())) {
            closeClient(ioTMessage.getConnectionId());
        }

    }

    protected Channel getChannel(String channelId) {
        return getChannelGroup().find(channelId);
    }

    protected abstract ServerInitializer<T> getServerInitializer(ServerImpl<T> serverImpl, int connectionTimeout);

    protected abstract ServerInitializer<T> getServerInitializer(ServerImpl<T> serverImpl, int connectionTimeout, SSLHandler sslHandler);

 }
