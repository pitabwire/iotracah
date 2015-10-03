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

package com.caricah.iotracah.server.httpserver.netty;

import com.caricah.iotracah.core.modules.Server;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.exceptions.UnRetriableException;
import com.caricah.iotracah.server.netty.SSLHandler;
import com.caricah.iotracah.server.netty.ServerHandler;
import com.caricah.iotracah.server.netty.ServerImpl;
import com.caricah.iotracah.server.netty.ServerInitializer;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.http.FullHttpMessage;
import org.apache.commons.configuration.Configuration;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/28/15
 */
public class HttpServerImpl extends ServerImpl<FullHttpMessage> {


    public static final String CONFIGURATION_SERVER_HTTP_TCP_PORT = "system.internal.server.http.tcp.port";
    public static final int CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_TCP_PORT = 7180;

    public static final String CONFIGURATION_SERVER_HTTP_SSL_PORT = "system.internal.server.http.tcp.port";
    public static final int CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_SSL_PORT = 7183;

    public static final String CONFIGURATION_SERVER_HTTP_SSL_IS_ENABLED = "system.internal.server.http.ssl.is.enabled";
    public static final boolean CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_SSL_IS_ENABLED = true;

    public static final String CONFIGURATION_SERVER_HTTP_CONNECTION_TIMEOUT = "system.internal.server.http.connection.timeout";
    public static final int CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_CONNECTION_TIMEOUT = 10;

    public HttpServerImpl(Server<FullHttpMessage> internalServer) {
        super(internalServer);
    }

    /**
     * @param configuration Object carrying all configurable properties from file.
     * @throws UnRetriableException
     * @link configure method supplies the configuration object carrying all the
     * properties parsed from the external properties file.
     */

    public void configure(Configuration configuration) throws UnRetriableException {
        log.info(" configure : setting up our configurations.");

        int tcpPort = configuration.getInt(CONFIGURATION_SERVER_HTTP_TCP_PORT, CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_TCP_PORT);
        setTcpPort(tcpPort);

        int sslPort = configuration.getInt(CONFIGURATION_SERVER_HTTP_SSL_PORT, CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_SSL_PORT);
        setSslPort(sslPort);

        boolean sslEnabled = configuration.getBoolean(CONFIGURATION_SERVER_HTTP_SSL_IS_ENABLED, CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_SSL_IS_ENABLED);
        setSslEnabled(sslEnabled);

        if(isSslEnabled()){

            setSslHandler(new SSLHandler(configuration));

        }

        int connectionTimeout = configuration.getInt(CONFIGURATION_SERVER_HTTP_CONNECTION_TIMEOUT, CONFIGURATION_VALUE_DEFAULT_SERVER_HTTP_CONNECTION_TIMEOUT);
        setConnectionTimeout(connectionTimeout);

    }


    @Override
    protected ServerInitializer<FullHttpMessage> getServerInitializer(ServerImpl<FullHttpMessage> serverImpl, int connectionTimeout) {
        return new HttpServerInitializer(serverImpl, connectionTimeout);
    }

    @Override
    protected ServerInitializer<FullHttpMessage> getServerInitializer(ServerImpl<FullHttpMessage> serverImpl, int connectionTimeout, SSLHandler sslHandler) {
        return new HttpServerInitializer(serverImpl, connectionTimeout,sslHandler);
    }



    @Override
    public void postProcess(IOTMessage ioTMessage) {

        //Always close the connection once there is a response.
        closeClient((ChannelId)ioTMessage.getConnectionId());


    }
}
