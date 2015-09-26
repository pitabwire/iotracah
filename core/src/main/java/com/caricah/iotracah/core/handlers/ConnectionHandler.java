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

package com.caricah.iotracah.core.handlers;

import com.caricah.iotracah.core.modules.Worker;
import com.caricah.iotracah.core.worker.exceptions.ShutdownException;
import com.caricah.iotracah.core.worker.exceptions.UnknownProtocalException;
import com.caricah.iotracah.core.worker.state.SessionResetManager;
import com.caricah.iotracah.core.worker.state.messages.ConnectAcknowledgeMessage;
import com.caricah.iotracah.core.worker.state.messages.ConnectMessage;
import com.caricah.iotracah.core.worker.state.messages.WillMessage;
import com.caricah.iotracah.core.worker.state.models.Client;
import com.caricah.iotracah.exceptions.RetriableException;
import com.caricah.iotracah.exceptions.UnRetriableException;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttIdentifierRejectedException;
import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;
import io.netty.handler.codec.mqtt.MqttVersion;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class ConnectionHandler extends RequestHandler {

    private final Pattern partitionPattern = Pattern.compile("(?<partition>.*)-<(?<clientId>.*)>");
    private final Pattern pattern = Pattern.compile("[-/<>a-zA-Z0-9_]*");

    private ConnectMessage message;


    public ConnectionHandler(ConnectMessage message) {
        this.message = message;
    }


    /**
     * * 3.1.4 Response
     * <p>
     * Note that a Server MAY support multiple protocols (including earlier versions of this protocol)
     * on the same TCP port or other network endpoint. If the Server determines that the protocol is MQTT 3.1.1
     * then it validates the connection attempt as follows.
     * <p>
     * 1.     If the Server does not receive a CONNECT Packet within a reasonable amount of time after
     * the Network Connection is established, the Server SHOULD close the connection.
     * <p>
     * 2.     The Server MUST validate that the CONNECT Packet conforms to section 3.1 and close
     * the Network Connection without sending a CONNACK if it does not conform [MQTT-3.1.4-1].
     * <p>
     * 3.     The Server MAY check that the contents of the CONNECT Packet meet any further restrictions
     * and MAY perform authentication and authorization checks. If any of these checks fail,
     * it SHOULD send an appropriate CONNACK response with a non-zero return code as described
     * in section 3.2 and it MUST close the Network Connection.
     * <p>
     * If validation is successful the Server performs the following steps.
     * <p>
     * 1.     If the ClientId represents a Client already connected to the Server then
     * the Server MUST disconnect the existing Client [MQTT-3.1.4-2].
     * 2.     The Server MUST perform the processing of CleanSession that is described
     * in section 3.1.2.4 [MQTT-3.1.4-3].
     * 3.     The Server MUST acknowledge the CONNECT Packet with a CONNACK Packet
     * containing a zero return code [MQTT-3.1.4-4].
     * 4.     Start message delivery and keep alive monitoring.
     * <p>
     * Clients are allowed to send further Control Packets immediately after sending a CONNECT Packet;
     * Clients need not wait for a CONNACK Packet to arrive from the Server. If the Server rejects the CONNECT,
     * it MUST NOT process any data sent by the Client after the CONNECT Packet [MQTT-3.1.4-5].
     * <p>
     * Non normative comment
     * Clients typically wait for a CONNACK Packet,
     * However, if the Client exploits its freedom to send Control Packets before it receives a CONNACK,
     * it might simplify the Client implementation as it does not have to police the connected state.
     * The Client accepts that any data that it sends before it receives a CONNACK packet from the
     * Server will not be processed if the Server rejects the connection.
     *
     * @return
     * @throws RetriableException
     * @throws UnRetriableException
     */
    @Override
    public void handle() throws RetriableException, UnRetriableException {


        log.debug(" handle : client initiating a new connection.");

        /**
         * 2.     The Server MUST validate that the CONNECT Packet conforms to section 3.1 and close
         *        the Network Connection without sending a CONNACK if it does not conform [MQTT-3.1.4-1].
         *
         *        3.1[ The Server MUST process a second CONNECT Packet sent from a Client as a protocol
         *        violation and disconnect the Client [MQTT-3.1.0-2].  See section 4.8 for information about handling errors.]
         *
         *
         */


        try {

            if (!MqttVersion.MQTT_3_1_1.protocolName().equals(message.getProtocolName())
                    && !MqttVersion.MQTT_3_1.protocolName().equals(message.getProtocolName())
                    ) {

                /**
                 * If the protocol name is incorrect the Server MAY disconnect the Client,
                 * or it MAY continue processing the CONNECT packet in accordance with some other specification.
                 * In the latter case, the Server MUST NOT continue to process the CONNECT packet in line with
                 * this specification [MQTT-3.1.2-1].
                 *
                 */

                throw new UnknownProtocalException();

            }


            if (MqttVersion.MQTT_3_1_1.protocolLevel() != message.getProtocalLevel()
                    && MqttVersion.MQTT_3_1.protocolLevel() != message.getProtocalLevel()
                    ) {

                /**
                 * The 8 bit unsigned value that represents the revision level of the protocol used by the Client.
                 * The value of the Protocol Level field for the version 3.1.1 of the protocol is 4 (0x04).
                 * The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01 (unacceptable protocol level)
                 * and then disconnect the Client if the Protocol Level is not supported by the Server [MQTT-3.1.2-2].
                 */

                throw new MqttUnacceptableProtocolVersionException();
            } else {
                log.debug(" handle: the required protocal was selected.");
            }


            //TODO:      The Server MUST validate that the reserved flag in the CONNECT Control Packet is set to zero and disconnect the Client if it is not zero [MQTT-3.1.2-3].


            //We now proceed to openning a session on our core service interface.
            boolean cleanSession = message.isCleanSession();

            /**
             * The Server MUST allow ClientIds which are between 1 and 23 UTF-8 encoded bytes in length,
             * and that contain only the characters
             *           "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" [MQTT-3.1.3-5].
             *
             * The Server MAY allow ClientId’s that contain more than 23 encoded bytes.
             * The Server MAY allow ClientId’s that contain characters not included in the list given above.
             *
             *
             * A Server MAY allow a Client to supply a ClientId that has a length of zero bytes,
             * however if it does so the Server MUST treat this as a special case and assign a unique ClientId to that Client.
             * It MUST then process the CONNECT packet as if the Client had provided that unique ClientId [MQTT-3.1.3-6].
             *
             */
            String clientIdentifier = message.getClientIdentifier();


            /**
             * If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1 [MQTT-3.1.3-7].
             *
             * If the Client supplies a zero-byte ClientId with CleanSession set to 0,
             * the Server MUST respond to the CONNECT Packet with a CONNACK return code 0x02 (Identifier rejected)
             * and then close the Network Connection [MQTT-3.1.3-8].
             */
            if ((null == clientIdentifier || clientIdentifier.isEmpty())) {

                if (!cleanSession) {

                    throw new MqttIdentifierRejectedException();
                }
            } else {

                //Run a regular expression to check for invalid characters in our clientIdentifier.
                if (!pattern.matcher(clientIdentifier).matches()) {

                    throw new MqttIdentifierRejectedException();

                }

            }

            log.debug(" handle: we are ready now to obtain the core session.");

            Observable<Client> newClientObservable = openSubject(getWorker(), message.getCluster(), message.getNodeId(),
                    message.getConnectionId(), clientIdentifier, cleanSession, message.getUserName(),
                    message.getPassword(), message.getKeepAliveTime(), message.getSourceHost());

            newClientObservable.first().subscribeOn(Schedulers.io()).subscribe(

                    new Subscriber<Client>() {
                        @Override
                        public void onCompleted() {
                        }

                        @Override
                        public void onError(Throwable e) {

                            log.error(" onError : Problems ", e);

                            ConnectAcknowledgeMessage connectAcknowledgeMessage;

                            if (e instanceof AuthenticationException) {

                                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);

                            } else if (e instanceof AuthorizationException) {

                                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);

                            } else {
                                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
                            }

                            connectAcknowledgeMessage.copyBase(message);
                            pushToServer(connectAcknowledgeMessage);


                        }

                        @Override
                        public void onNext(Client client) {

                            message.setClientIdentifier(client.getClientIdentifier());
                            message.setPartition(client.getPartition());

                            log.debug(" handle: obtained a client : {}. ", client);

                            /**
                             * 3.     The Server MAY check that the contents of the CONNECT Packet meet any further restrictions
                             *        and MAY perform authentication and authorization checks. If any of these checks fail,
                             *        it SHOULD send an appropriate CONNACK response with a non-zero return code as described
                             *      in section 3.2 and it MUST close the Network Connection.
                             *
                             */


                            log.info(" onNext : Successfully initiated a session.");

                            //Respond to server with a connection successfull.
                            ConnectAcknowledgeMessage connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_ACCEPTED);
                            connectAcknowledgeMessage.copyBase(message);
                            pushToServer(connectAcknowledgeMessage);


                            WillMessage will;
                            if (message.isHasWill()) {

                                /**
                                 * If the Will Flag is set to 1 this indicates that, if the Connect request is accepted,
                                 * a Will Message MUST be stored on the Server and associated with the Network Connection.
                                 * The Will Message MUST be published when the Network Connection is subsequently closed unless
                                 * the Will Message has been deleted by the Server on receipt of a DISCONNECT Packet [MQTT-3.1.2-8].
                                 */


                                will = WillMessage.from(message.isRetainWill(), message.getWillQos(),
                                        message.getWillTopic(), message.getWillMessage());
                                will.copyBase(message);

                                getDatastore().saveWill(will);
                            } else {
                                //We need to clear the existing will message.
                                will = WillMessage.from(false, 0, "", "");
                                will.setPartition(message.getPartition());
                                will.setClientIdentifier(message.getClientIdentifier());
                                will.copyBase(message);
                                getDatastore().removeWill(will);
                            }

                            //Perform a reset for our session.
                            SessionResetManager resetManager = getWorker().getSessionResetManager();
                            resetManager.process(client);


                        }
                    }


            );
        } catch (MqttUnacceptableProtocolVersionException | MqttIdentifierRejectedException | AuthenticationException | UnknownProtocalException e) {

            log.debug(" handle : Client connection issues ", e);

            //Respond to server with a connection successfull.
            ConnectAcknowledgeMessage connectAcknowledgeMessage;

            if (e instanceof MqttIdentifierRejectedException) {
                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);

            } else if (e instanceof MqttUnacceptableProtocolVersionException) {

                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);

            } else if (e instanceof UnknownProtocalException) {

                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION);

            } else {
                connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            }

            connectAcknowledgeMessage.copyBase(message);
            throw new ShutdownException(connectAcknowledgeMessage);

        } catch (Exception systemError) {

            ConnectAcknowledgeMessage connectAcknowledgeMessage = ConnectAcknowledgeMessage.from(message.isDup(), message.getQos(), message.isRetain(), message.getKeepAliveTime(), MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);
            connectAcknowledgeMessage.copyBase(message);
            log.error(" handle : System experienced the error ", systemError);
            throw new ShutdownException(connectAcknowledgeMessage);

        }

    }

    private Observable<Client> openSubject(Worker worker, String connectedCluster, UUID connectedNode,
                                           Serializable connectionID, String clientIdentifier, boolean cleanSession,
                                           String userName, String password, int keepAliveTime, String sourceHost) {

        return Observable.create(observable -> {

            try {

                log.debug(" openSubject : create -- initiating subject creation.");

                String activeClientId = clientIdentifier;
                if (null == clientIdentifier) {
                    activeClientId = worker.getDatastore().nextClientId();
                }

                String partition = userName;
                if (!getDatastore().isPartitionBasedOnUsername()) {
                    partition = processClientIdForPartition(activeClientId);
                }

                Client defaultClient = new Client();
                defaultClient.setConnectedCluster(connectedCluster);
                defaultClient.setConnectedNode(connectedNode);
                defaultClient.setConnectionId(connectionID);
                defaultClient.setClientIdentifier(activeClientId);
                defaultClient.setPartition(partition);
                defaultClient.setSessionId(null);
                defaultClient.setActive(false);

                log.debug(" openSubject : create -- Futher into the database.");

                Observable<Client> clientObservable = getDatastore().getClient(partition, activeClientId);
                clientObservable.firstOrDefault(defaultClient).subscribe(new Subscriber<Client>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        observable.onError(e);
                    }

                    @Override
                    public void onNext(Client client) {


                        //We have obtained a client to work with.

                        log.debug(" openSubject : create -- We obtained a client.");

                        try {


                            PrincipalCollection principals = new SimplePrincipalCollection(client.getClientIdentifier(), "");

                            Subject.Builder subjectBuilder = new Subject.Builder();
                            subjectBuilder = subjectBuilder.principals(principals);
                            subjectBuilder = subjectBuilder.host(sourceHost);
                            subjectBuilder = subjectBuilder.sessionCreationEnabled(true);
                            subjectBuilder = subjectBuilder.sessionId(client.getSessionId());
                            Subject activeUser = subjectBuilder.buildSubject();

                            if (activeUser.isAuthenticated() && cleanSession) {
                                //Clean a logged in session.
                                activeUser.logout();
                            }

                            UsernamePasswordToken token = new UsernamePasswordToken(userName, password);


                            activeUser.login(token);

                            log.info(" openSubject : Authenticated client <{}> username {} ", client.getClientIdentifier(), userName);

                            Session session = activeUser.getSession();

                            Double keepAliveDisconnectiontime = keepAliveTime * 1.5;

                            session.setTimeout(keepAliveDisconnectiontime.intValue());
                            session.touch();


                            client.setActive(true);
                            client.setCleanSession(cleanSession);
                            client.setSessionId(session.getId());

                            getDatastore().saveClient(client);

                            observable.onNext(client);
                            observable.onCompleted();

                        } catch (Exception e) {
                            observable.onError(e);
                        }
                    }
                });


            } catch (Exception e) {
                observable.onError(e);
            }

        });
    }

    private String processClientIdForPartition(String activeClientId) {

        Matcher matcher = partitionPattern.matcher(activeClientId);
        if(matcher.matches()) {
            return matcher.group("partition");
        }else {
            return "";
        }

    }


}
