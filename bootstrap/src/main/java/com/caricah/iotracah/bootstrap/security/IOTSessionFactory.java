/*
 *
 * Copyright (c) 2016 Caricah <info@caricah.com>.
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

package com.caricah.iotracah.bootstrap.security;

import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.SessionContext;
import org.apache.shiro.session.mgt.SessionFactory;

/**
 *
 * {@code SessionFactory} implementation that generates {@link IOTClient} instances.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 1/16/16
 */
public class IOTSessionFactory  implements SessionFactory {

        /**
         * Creates a new {@link IOTClient} instance retaining the context's
         * {@link SessionContext#getHost() host} if one can be found.
         *
         * @param initData the initialization data to be used during {@link Session} creation.
         * @return a new {@link IOTClient} instance
         */
        public Session createSession(SessionContext initData) {
            if (initData != null) {
                IOTClient iotClient = new IOTClient(initData.getHost());
                iotClient.setPartitionId((String) initData.get(IOTClient.CONTEXT_PARTITION_KEY));
                iotClient.setUsername((String) initData.get(IOTClient.CONTEXT_USERNAME_KEY));
                iotClient.setClientIdentification((String) initData.get(IOTClient.CONTEXT_CLIENT_ID_KEY));

                String  sessionId = IOTClient.keyFromStrings(iotClient.getPartitionId(), iotClient.getClientIdentification()).getSessionId();
                iotClient.setSessionId(sessionId);
                iotClient.setIsActive(true);
                iotClient.setIsExpired(false);

                return iotClient;
            }
            return null;
        }

}
