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

package com.caricah.iotracah.server.httpserver.transform;

import com.caricah.iotracah.core.worker.state.messages.*;
import com.caricah.iotracah.core.worker.state.messages.base.IOTMessage;
import com.caricah.iotracah.server.transform.IOTMqttTransformer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 9/23/15
 */
public class IOTHttpTransformerImpl implements IOTMqttTransformer<FullHttpMessage> {


    @Override
    public FullHttpMessage toServerMessage(IOTMessage internalMessage) {

        JSONObject json = new JSONObject();

        switch (internalMessage.getMessageType()) {

            case AcknowledgeMessage.MESSAGE_TYPE:
                AcknowledgeMessage ackMsg = (AcknowledgeMessage) internalMessage;

                json.put("messageId", ackMsg.getMessageId());
                json.put("qos", ackMsg.getQos());
                json.put("message", "published");
            break;
            case ConnectAcknowledgeMessage.MESSAGE_TYPE:
                ConnectAcknowledgeMessage conAck = (ConnectAcknowledgeMessage) internalMessage;

                json.put("sessionId", conAck.getSessionId() );
                json.put("authKey", conAck.getAuthKey());
                json.put("message", conAck.getReturnCode().name() );

            break;
            case SubscribeAcknowledgeMessage.MESSAGE_TYPE:
                SubscribeAcknowledgeMessage subAck = (SubscribeAcknowledgeMessage) internalMessage;


                json.put("message", "subscribed");
                final JSONArray jsonGrantedQos = new JSONArray();
                subAck.getGrantedQos().forEach(jsonGrantedQos::put);
                json.put("grantedQos", jsonGrantedQos);
                break;

            case UnSubscribeAcknowledgeMessage.MESSAGE_TYPE:
                UnSubscribeAcknowledgeMessage unSubAck = (UnSubscribeAcknowledgeMessage) internalMessage;

                json.put("message", "unsubscribed" );

                break;
            case DisconnectMessage.MESSAGE_TYPE:

                DisconnectMessage discMsg = (DisconnectMessage) internalMessage;

                json.put("sessionId", discMsg.getSessionId() );
                json.put("message", "disconnected" );

                break;
            default:
                /**
                 *
                 * Internally these are not expected to get here.
                 * In such cases we just return a null
                 * and log this anomaly as a gross error.
                 *
                 **/


                json.put("message","UnExpected outcome");
                break;
        }


        ByteBuf buffer = Unpooled.copiedBuffer(json.toString(), CharsetUtil.UTF_8);

        // Build the response object.
        FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, buffer);

        httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, buffer.readableBytes());
        return httpResponse;


    }
}
