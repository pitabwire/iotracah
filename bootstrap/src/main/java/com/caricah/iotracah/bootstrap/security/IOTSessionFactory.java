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

import com.caricah.iotracah.bootstrap.security.realm.state.IOTSession;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.SessionContext;
import org.apache.shiro.session.mgt.SessionFactory;
import org.apache.shiro.session.mgt.SimpleSession;

/**
 *
 * {@code SessionFactory} implementation that generates {@link IOTSession} instances.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 1/16/16
 */
public class IOTSessionFactory  implements SessionFactory {

        /**
         * Creates a new {@link IOTSession} instance retaining the context's
         * {@link SessionContext#getHost() host} if one can be found.
         *
         * @param initData the initialization data to be used during {@link Session} creation.
         * @return a new {@link IOTSession} instance
         */
        public Session createSession(SessionContext initData) {
            if (initData != null) {
                IOTSession iotSession = new IOTSession(initData.getHost());
                iotSession.setPartition( (String) initData.get(IOTSession.CONTEXT_PARTITION_KEY));
                iotSession.setUsername( (String) initData.get(IOTSession.CONTEXT_USERNAME_KEY));
                iotSession.setClientId((String) initData.get(IOTSession.CONTEXT_CLIENT_ID_KEY));

                return iotSession;
            }
            return null;
        }

}
