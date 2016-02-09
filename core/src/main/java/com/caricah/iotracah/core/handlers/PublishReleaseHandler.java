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

import com.caricah.iotracah.bootstrap.security.realm.state.IOTSession;
import com.caricah.iotracah.core.security.AuthorityRole;
import com.caricah.iotracah.bootstrap.data.messages.CompleteMessage;
import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.messages.ReleaseMessage;
import com.caricah.iotracah.bootstrap.exceptions.RetriableException;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import rx.Observable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 */
public class PublishReleaseHandler extends RequestHandler<ReleaseMessage> {

    @Override
    public void handle(ReleaseMessage releaseMessage) throws RetriableException, UnRetriableException {

        //Check for connect permissions
        Observable<IOTSession> permissionObservable = checkPermission(releaseMessage.getSessionId(),
                releaseMessage.getAuthKey(), AuthorityRole.CONNECT);
        permissionObservable.subscribe(

                (iotSession) -> {


                    /**
                     * MUST respond to a PUBREL packet by sending a PUBCOMP packet containing the same Packet Identifier as the PUBREL.
                     * After it has sent a PUBCOMP, the receiver MUST treat any subsequent PUBLISH packet that contains that Packet Identifier as being a new publication.
                     */


                    Observable<PublishMessage> messageObservable = getDatastore().getMessage(
                            iotSession, releaseMessage.getMessageId(), true);

                    messageObservable.subscribe(publishMessage -> {

                        publishMessage.setReleased(true);
                        Observable<Long> messageIdObservable = getDatastore().saveMessage(publishMessage);

                        messageIdObservable.subscribe(messageId -> {
                            try {

                                getMessenger().publish(iotSession.getPartition(), publishMessage);
                                //Initiate a publish complete.
                                CompleteMessage destroyMessage = CompleteMessage.from(publishMessage.getMessageId());
                                destroyMessage.copyBase(publishMessage);
                                pushToServer(destroyMessage);


                                //Destroy message.
                                getDatastore().removeMessage(publishMessage);

                            } catch (RetriableException e) {
                                log.error(" releaseInboundMessage : encountered a problem while publishing.", e);
                            }
                        });
                    });
                }, throwable -> disconnectDueToError(throwable, releaseMessage));
    }


}
