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

package com.caricah.iotracah.core.worker;

import com.caricah.iotracah.core.handlers.*;
import com.caricah.iotracah.core.worker.exceptions.ShutdownException;
import com.caricah.iotracah.core.worker.state.messages.*;
import com.caricah.iotracah.core.worker.state.messages.base.Protocal;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.worker.state.SessionResetManager;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 8/15/15
 */
public class DefaultWorker extends Worker {
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

    }

    /**
     * <code>initiate</code> starts the operations of this system handler.
     * All excecution code for the plugins is expected to begin at this point.
     *
     * @throws UnRetriableException
     */
    @Override
    public void initiate() throws UnRetriableException {

        //Initiate the session reset manager.
        SessionResetManager sessionResetManager = new SessionResetManager();
        sessionResetManager.setWorker(this);
        sessionResetManager.setDatastore(this.getDatastore());
        setSessionResetManager(sessionResetManager);



        //



    }

    /**
     * <code>terminate</code> halts excecution of this plugin.
     * This provides a clean way to exit /stop operations of this particular plugin.
     */
    @Override
    public void terminate() {

    }

    /**
     * Provides the Observer with a new item to observe.
     * <p>
     * The {@link com.caricah.iotracah.core.modules.Server} may call this method 0 or more times.
     * <p>
     * The {@code Observable} will not call this method again after it calls either {@link #onCompleted} or
     * {@link #onError}.
     *
     * @param iotMessage the item emitted by the Observable
     */
    @Override
    public void onNext(IOTMessage iotMessage) {


        RequestHandler requestHandler = getHandlerForMessage(iotMessage);
        try {

            if(null != requestHandler) {
                requestHandler.handle();
            }else {

                throw new ShutdownException("Unknown messages being propergated");
            }
        } catch (ShutdownException e) {

            IOTMessage response = ((ShutdownException) e).getResponse();
            if (null != response) {
                pushToServer(response);
            }





        } catch (Exception e) {
            logError(" onNext : Serious error that requires attention ", e);
        }

    }

    private RequestHandler getHandlerForMessage(IOTMessage iotMessage) {

        RequestHandler requestHandler = null;

        if(iotMessage instanceof ConnectMessage){
            requestHandler = new ConnectionHandler((ConnectMessage)iotMessage);
        }else if(iotMessage instanceof SubscribeMessage){
            requestHandler = new SubscribeHandler((SubscribeMessage) iotMessage);
        }else if(iotMessage instanceof UnSubscribeMessage){
            requestHandler = new UnSubscribeHandler((UnSubscribeMessage) iotMessage);
        }else if(iotMessage instanceof Ping){
            requestHandler = new PingRequestHandler((Ping) iotMessage);
        }else if(iotMessage instanceof PublishMessage) {
            requestHandler = new PublishInHandler((PublishMessage) iotMessage);

        }else  if(iotMessage instanceof PublishReceivedMessage) {
            requestHandler = new PublishReceivedHandler((PublishReceivedMessage)iotMessage);

        }else if(iotMessage instanceof ReleaseMessage){
            requestHandler = new PublishReleaseHandler((ReleaseMessage) iotMessage);
        }else if(iotMessage instanceof DestroyMessage){
            requestHandler = new PublishCompleteHandler((DestroyMessage) iotMessage);
        }else if(iotMessage instanceof DisconnectMessage){
            requestHandler = new DisconnectHandler((DisconnectMessage) iotMessage);
        } else if(iotMessage instanceof AcknowledgeMessage){
                requestHandler = new PublishAcknowledgeHandler((AcknowledgeMessage) iotMessage);
        }
        return requestHandler;
    }

    @Override
    public Protocal getProtocal() {
        return Protocal.MQTT;
    }
}
