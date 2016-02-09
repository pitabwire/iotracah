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
import org.apache.shiro.session.InvalidSessionException;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.SessionException;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.session.mgt.SessionContext;
import org.apache.shiro.session.mgt.SessionKey;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 2/4/16
 */
public class IOTSessionManager extends DefaultSessionManager {

    public Session start(SessionContext context) {
        IOTSession session = (IOTSession) createSession(context);

        session.setSessionManager(this);
        applyGlobalSessionTimeout(session);

        notifyStart(session);
        return session;
    }


    public void stop(SessionKey key) throws InvalidSessionException {

        IOTSession session = (IOTSession) getSession(key);
        session.setSessionManager(this);

        try {
            notifyStop(session);
        } finally {
            afterStopped(session);
        }
    }


    public Session getSession(SessionKey key) throws SessionException {
        if (key == null) {
            throw new NullPointerException("SessionKey argument cannot be null.");
        }
        IOTSession session = (IOTSession) doGetSession(key);
        session.setSessionManager(this);
        return session;
    }


    @Override
    protected void validate(Session session, SessionKey key) throws InvalidSessionException {
        ((IOTSession) session).setSessionManager(this);
        super.validate(session, key);
    }

    @Override
    protected Session beforeInvalidNotification(Session session) {
        return session;
    }

    @Override
    protected void afterStopped(Session session) {

        if (((IOTSession) session).isCleanSession()) {
            delete(session);
        }
    }

}
