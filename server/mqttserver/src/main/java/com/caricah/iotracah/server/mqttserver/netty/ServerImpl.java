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
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.server.mqttserver.MqttServer;
import com.caricah.iotracah.server.mqttserver.ServerInterface;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.commons.configuration.Configuration;
import org.apache.shiro.subject.Subject;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/26/15
 */
public class ServerImpl implements ServerInterface {

    private final MqttServer internalServer;

    public static final String CONFIGURATION_SERVER_MQTT_TCP_PORT = "system.internal.server.mqtt.tcp.port";
    public static final int CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_TCP_PORT = 1883;

    public static final String CONFIGURATION_SERVER_MQTT_SSL_PORT = "system.internal.server.mqtt.tcp.port";
    public static final int CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_SSL_PORT = 8883;

    public static final String CONFIGURATION_SERVER_MQTT_SSL_IS_ENABLED = "system.internal.server.mqtt.ssl.is.enabled";
    public static final boolean CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_SSL_IS_ENABLED = true;

    public static final String CONFIGURATION_SERVER_MQTT_CONNECTION_TIMEOUT = "system.internal.server.mqtt.connection.timeout";
    public static final int CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_CONNECTION_TIMEOUT = 10;

    public static final AttributeKey<String> REQUEST_CLIENT_ID = AttributeKey.valueOf("requestClientIdKey");
    public static final AttributeKey<Serializable> REQUEST_SESSION_ID = AttributeKey.valueOf("requestSessionIdKey");
    public static final AttributeKey<Serializable> REQUEST_CONNECTION_ID = AttributeKey.valueOf("requestConnectionIdKey");



    private int tcpPort = CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_TCP_PORT;
    private int sslPort = CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_SSL_PORT;
    private boolean sslEnabled = CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_SSL_IS_ENABLED;
    private int connectionTimeout = CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_CONNECTION_TIMEOUT;

    private SSLHandler sslHandler = null;

    private EventLoopGroup parentGroup = null;
    private EventLoopGroup childGroup = null;

    private Channel tcpChannel = null;
    private Channel sslChannel = null;

    private final ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    public ChannelGroup getChannelGroup() {
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


    public ServerImpl(MqttServer internalServer) {
        this.internalServer = internalServer;
    }

    public MqttServer getInternalServer() {
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

        getInternalServer().logInfo(" configure : initiating the netty server.");

        try {

            parentGroup = new NioEventLoopGroup(1);
            childGroup = new NioEventLoopGroup();



            //Initialize listener for TCP
            ServerBootstrap tcpBootstrap = new ServerBootstrap();
            tcpBootstrap.group(parentGroup, childGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ServerInitializer(this, getConnectionTimeout()));

            ChannelFuture tcpChannelFuture = tcpBootstrap.bind(getTcpPort()).sync();
            tcpChannel = tcpChannelFuture.channel();


            if (isSslEnabled()) {
                //Initialize listener for SSL
                ServerBootstrap sslBootstrap = new ServerBootstrap();
                sslBootstrap.group(parentGroup, childGroup)
                        .channel(NioServerSocketChannel.class)
                        .handler(new LoggingHandler(LogLevel.INFO))
                        .childHandler(new ServerInitializer(this, getConnectionTimeout(), getSslHandler()));

                ChannelFuture sslChannelFuture = sslBootstrap.bind(getSslPort()).sync();
                sslChannel = sslChannelFuture.channel();
            }

        }catch (InterruptedException e){

            getInternalServer().logError(" configure : Initialization issues ", e);

            throw new UnRetriableException(e);

        }


    }

    /**
     * @param configuration Object carrying all configurable properties from file.
     * @throws UnRetriableException
     * @link configure method supplies the configuration object carrying all the
     * properties parsed from the external properties file.
     */

    public void configure(Configuration configuration) throws UnRetriableException {
        getInternalServer().logInfo(" configure : setting up our configurations.");

        int tcpPort = configuration.getInt(CONFIGURATION_SERVER_MQTT_TCP_PORT, CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_TCP_PORT);
        setTcpPort(tcpPort);

        int sslPort = configuration.getInt(CONFIGURATION_SERVER_MQTT_SSL_PORT, CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_SSL_PORT);
        setSslPort(sslPort);

        boolean sslEnabled = configuration.getBoolean(CONFIGURATION_SERVER_MQTT_SSL_IS_ENABLED, CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_SSL_IS_ENABLED);
        setSslEnabled(sslEnabled);

        if(isSslEnabled()){

            setSslHandler(new SSLHandler(configuration));

        }

        int connectionTimeout = configuration.getInt(CONFIGURATION_SERVER_MQTT_CONNECTION_TIMEOUT, CONFIGURATION_VALUE_DEFAULT_SERVER_MQTT_CONNECTION_TIMEOUT);
        setConnectionTimeout(connectionTimeout);

    }

    /**
     * @link terminate method is expected to cleanly shut down the server implementation and return immediately.
     */
    public void terminate() {
        getInternalServer().logInfo(" terminate : stopping any processing. ");

        //Stop all connections.
        getChannelGroup().close().awaitUninterruptibly();


        if(null != sslChannel){
            sslChannel.close().awaitUninterruptibly();
        }

        if(null != tcpChannel){
            tcpChannel.close().awaitUninterruptibly();
        }

        if(null != childGroup){
            childGroup.shutdownGracefully();
        }

        if(null != parentGroup){
            parentGroup.shutdownGracefully();
        }

    }


    public void pushToClient(Serializable connectionId, Serializable sessionId, String clientId, MqttMessage message){


    }
}
