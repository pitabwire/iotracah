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

package com.caricah.iotracah.server.mqttserver.transform;

import com.caricah.iotracah.bootstrap.data.messages.*;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.server.transform.IOTMqttTransformer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/23/15
 */
public class IOTMqttTransformerImpl implements IOTMqttTransformer<MqttMessage> {
    @Override
    public MqttMessage toServerMessage(IOTMessage internalMessage) {


        switch (internalMessage.getMessageType()) {

            case PublishMessage.MESSAGE_TYPE:


                PublishMessage pubMsg = (PublishMessage) internalMessage;
                //We generate a publish message.
                MqttPublishVariableHeader respVariableHeader = new MqttPublishVariableHeader(pubMsg.getTopic(), pubMsg.getMessageId());

                MqttFixedHeader respFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, pubMsg.getIsDuplicate(), MqttQoS.valueOf(pubMsg.getQos()), pubMsg.getIsRetain(), 0);
                ByteBuf content = Unpooled.wrappedBuffer((byte[]) pubMsg.getPayload());

                return MqttMessageFactory.newMessage(respFixedHeader, respVariableHeader, content);

            case AcknowledgeMessage.MESSAGE_TYPE:

                //Generate a PUBACK for qos 1 messages.
                AcknowledgeMessage ackMsg = (AcknowledgeMessage) internalMessage;
                MqttFixedHeader ackFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, ackMsg.isDup(), MqttQoS.valueOf(ackMsg.getQos()), ackMsg.isRetain(), 0);
                MqttMessageIdVariableHeader msgIdVariableHeader = MqttMessageIdVariableHeader.from(ackMsg.getMessageId());
                return MqttMessageFactory.newMessage(ackFixedHeader, msgIdVariableHeader, null);


            case PublishReceivedMessage.MESSAGE_TYPE:

                //We need to generate a PUBREC message to acknowledge reception of message.
                PublishReceivedMessage pubrec = (PublishReceivedMessage) internalMessage;
                MqttFixedHeader recFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREC, false, MqttQoS.valueOf(pubrec.getQos()), false, 0);
                msgIdVariableHeader = MqttMessageIdVariableHeader.from(pubrec.getMessageId());
                return MqttMessageFactory.newMessage(recFixedHeader, msgIdVariableHeader, null);


            case ReleaseMessage.MESSAGE_TYPE:

                //We need to generate a PUBREL message to release cached message.
                ReleaseMessage pubrel = (ReleaseMessage) internalMessage;
                MqttFixedHeader relFixedHeader = new MqttFixedHeader(MqttMessageType.PUBREL, pubrel.isDup(), MqttQoS.valueOf(pubrel.getQos()),false, 0);
                msgIdVariableHeader = MqttMessageIdVariableHeader.from(pubrel.getMessageId());
                return MqttMessageFactory.newMessage(relFixedHeader, msgIdVariableHeader, null);


            case CompleteMessage.MESSAGE_TYPE:

                //We need to generate a PUBCOMP message to acknowledge finalization of transmission of qos 2 message.
                CompleteMessage destroyMessage = (CompleteMessage) internalMessage;
                MqttFixedHeader compFixedHeader = new MqttFixedHeader(MqttMessageType.PUBCOMP, false, MqttQoS.valueOf(destroyMessage.getQos()), false, 0);
                msgIdVariableHeader = MqttMessageIdVariableHeader.from(destroyMessage.getMessageId());
                return MqttMessageFactory.newMessage(compFixedHeader, msgIdVariableHeader, null);

            case Ping.MESSAGE_TYPE:

                Ping ping = (Ping) internalMessage;
                //We need to generate a PINGRESP message to respond to a PINGREQ.
                recFixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, ping.isDup(), MqttQoS.valueOf(ping.getQos()), ping.isRetain(), 0);
                return MqttMessageFactory.newMessage(recFixedHeader, null, null);

            case ConnectAcknowledgeMessage.MESSAGE_TYPE:

                ConnectAcknowledgeMessage connAck = (ConnectAcknowledgeMessage) internalMessage;
                MqttFixedHeader connAckFixedHeader = new MqttFixedHeader(
                        MqttMessageType.CONNACK,
                        connAck.isDup(),
                        MqttQoS.valueOf(connAck.getQos()),
                        connAck.isRetain(), 0);
                MqttConnAckVariableHeader conAckVariableHeader = new MqttConnAckVariableHeader(connAck.getReturnCode());
                //Todo: Raise netty codec issue for lack of codec 3.2.2.2 Session Present flag.

                return MqttMessageFactory.newMessage(connAckFixedHeader, conAckVariableHeader, null);


            case SubscribeAcknowledgeMessage.MESSAGE_TYPE:

                SubscribeAcknowledgeMessage subAckMsg = (SubscribeAcknowledgeMessage) internalMessage;
                MqttSubAckPayload payload = new MqttSubAckPayload(subAckMsg.getGrantedQos());

                MqttFixedHeader subAckFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, subAckMsg.isDup(), MqttQoS.valueOf(subAckMsg.getQos()), subAckMsg.isRetain(), 0);
                MqttMessageIdVariableHeader subAckVariableHeader = MqttMessageIdVariableHeader.from(subAckMsg.getMessageId());
                return MqttMessageFactory.newMessage(subAckFixedHeader, subAckVariableHeader, payload);


            case UnSubscribeAcknowledgeMessage.MESSAGE_TYPE:

                UnSubscribeAcknowledgeMessage unSubAckMsg = (UnSubscribeAcknowledgeMessage) internalMessage;
                respFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, unSubAckMsg.isDup(), MqttQoS.valueOf(unSubAckMsg.getQos()), unSubAckMsg.isRetain(), 0);
                MqttMessageIdVariableHeader variableHeader = MqttMessageIdVariableHeader.from(unSubAckMsg.getMessageId());
                return MqttMessageFactory.newMessage(respFixedHeader, variableHeader, null);


            default:
                /**
                 *
                 * Internally these are not expected to get here.
                 * In such cases we just return a null
                 * and log this anomaly as a gross error.
                 *
                 *
                 * if(internalMessage instanceof ConnectMessage ||
                 * internalMessage instanceof SubscribeMessage ||
                 * internalMessage instanceof UnSubscribeMessage ||
                 * internalMessage instanceof DisconnectMessage ||
                 * internalMessage instanceof WillMessage ){
                 * }
                 **/



                return null;
        }


    }
}
