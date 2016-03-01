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

package com.caricah.iotracah.bootstrap.security;

import com.caricah.iotracah.bootstrap.security.realm.state.IOTClient;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.*;
import org.apache.shiro.mgt.DefaultSubjectFactory;
import org.apache.shiro.mgt.SessionsSecurityManager;
import org.apache.shiro.mgt.SubjectFactory;
import org.apache.shiro.session.InvalidSessionException;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.DefaultSessionKey;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.session.mgt.SessionKey;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;
import org.apache.shiro.subject.support.DefaultSubjectContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/10/15
 */
public class IOTSecurityManager extends SessionsSecurityManager {

    private static final Logger log = LoggerFactory.getLogger(IOTSecurityManager.class);

    protected SubjectFactory subjectFactory;

    public IOTSecurityManager(){
        super();

       setSessionManager(new IOTSessionManager());
        this.subjectFactory = new IOTSubjectFactory();

        ((IOTSessionManager) getSessionManager()).setSessionFactory(new IOTSessionFactory());

    }




    /**
     * Returns the {@code SubjectFactory} responsible for creating {@link Subject} instances exposed to the application.
     *
     * @return the {@code SubjectFactory} responsible for creating {@link Subject} instances exposed to the application.
     */
    public SubjectFactory getSubjectFactory() {
        return subjectFactory;
    }

    /**
     * Sets the {@code SubjectFactory} responsible for creating {@link Subject} instances exposed to the application.
     *
     * @param subjectFactory the {@code SubjectFactory} responsible for creating {@link Subject} instances exposed to the application.
     */
    public void setSubjectFactory(SubjectFactory subjectFactory) {
        this.subjectFactory = subjectFactory;
    }


    /**
     * Logs in the specified Subject using the given {@code authenticationToken}, returning an updated Subject
     * instance reflecting the authenticated state if successful or throwing {@code AuthenticationException} if it is
     * not.
     * <p>
     * Note that most application developers should probably not call this method directly unless they have a good
     * reason for doing so.  The preferred way to log in a Subject is to call
     * <code>subject.{@link Subject#login login(authenticationToken)}</code> (usually after
     * acquiring the Subject by calling {@link SecurityUtils#getSubject() SecurityUtils.getSubject()}).
     * <p>
     * Framework developers on the other hand might find calling this method directly useful in certain cases.
     *
     * @param subject             the subject against which the authentication attempt will occur
     * @param authenticationToken the token representing the Subject's principal(s) and credential(s)
     * @return the subject instance reflecting the authenticated state after a successful attempt
     * @throws AuthenticationException if the login attempt failed.
     * @since 1.0
     */
    @Override
    public Subject login(Subject subject, AuthenticationToken authenticationToken) throws AuthenticationException {

        AuthenticationInfo info = authenticate(authenticationToken);

        SubjectContext context = new DefaultSubjectContext();
        context.setAuthenticated(true);
        context.setAuthenticationToken(authenticationToken);
        context.setAuthenticationInfo(info);
        context.setSessionCreationEnabled(true);
        if (subject != null) {
            context.setSubject(subject);
        }

        return createSubject(context);
    }

    /**
     * Logs out the specified Subject from the system.
     * <p>
     * Note that most application developers should not call this method unless they have a good reason for doing
     * so.  The preferred way to logout a Subject is to call
     * <code>{@link Subject#logout Subject.logout()}</code>, not the
     * {@code SecurityManager} directly.
     * <p>
     * Framework developers on the other hand might find calling this method directly useful in certain cases.
     *
     * @param subject the subject to log out.
     * @since 1.0
     */
    @Override
    public void logout(Subject subject) {

        if (subject == null) {
            throw new IllegalArgumentException("Subject argument argument cannot be null.");
        }

        PrincipalCollection principals = subject.getPrincipals();
        if (principals != null && !principals.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Logging out subject with primary principal {}", principals.getPrimaryPrincipal());
            }
            Authenticator authc = getAuthenticator();
            if (authc instanceof LogoutAware) {
                ((LogoutAware) authc).onLogout(principals);
            }
        }

            try {
                stopSession(subject);
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    String msg = "Unable to cleanly stop Session for Subject [" + subject.getPrincipal() + "] " +
                            "Ignoring (logging out).";
                    log.debug(msg, e);
                }
            }


    }

    /**
     * Creates a {@code Subject} instance reflecting the specified contextual data.
     * <p>
     * The context can be anything needed by this {@code SecurityManager} to construct a {@code Subject} instance.
     * <h4>Usage</h4>
     * After calling this method, the returned instance is <em>not</em> bound to the application for further use.
     * Callers are expected to know that {@code Subject} instances have local scope only and any
     * other further use beyond the calling method must be managed explicitly.
     *
     * @param context any data needed to direct how the Subject should be constructed.
     * @return the {@code Subject} instance reflecting the specified initialization data.
     * @see Subject.Builder
     * @since 1.0
     */
    @Override
    public Subject createSubject(SubjectContext context) {


        //ensure that the context has a SecurityManager instance, and if not, add one:
        context = ensureSecurityManager(context);

        //Resolve an associated Session (usually based on a referenced session ID), and place it in the context before
        //sending to the SubjectFactory.  The SubjectFactory should not need to know how to acquire sessions as the
        //process is often environment specific - better to shield the SF from these details:
        context = resolveSession(context);


        //Resolve for

        return doCreateSubject(context);

    }

    /**
     * Actually creates a {@code Subject} instance by delegating to the internal
     * {@link #getSubjectFactory() subjectFactory}.  By the time this method is invoked, all possible
     * {@code SubjectContext} data (session, principals, et. al.) has been made accessible using all known heuristics
     * and will be accessible to the {@code subjectFactory} via the {@code subjectContext.resolve*} methods.
     *
     * @param context the populated context (data map) to be used by the {@code SubjectFactory} when creating a
     *                {@code Subject} instance.
     * @return a {@code Subject} instance reflecting the data in the specified {@code SubjectContext} data map.
     * @see #getSubjectFactory()
     * @see SubjectFactory#createSubject(org.apache.shiro.subject.SubjectContext)
     * @since 1.2
     */
    protected Subject doCreateSubject(SubjectContext context) {
        return getSubjectFactory().createSubject(context);
    }


    /**
     * Determines if there is a {@code SecurityManager} instance in the context, and if not, adds 'this' to the
     * context.  This ensures the SubjectFactory instance will have access to a SecurityManager during Subject
     * construction if necessary.
     *
     * @param context the subject context data that may contain a SecurityManager instance.
     * @return The SubjectContext to use to pass to a {@link SubjectFactory} for subject creation.
     * @since 1.0
     */
    @SuppressWarnings({"unchecked"})
    protected SubjectContext ensureSecurityManager(SubjectContext context) {
        if (context.resolveSecurityManager() != null) {
            log.trace("Context already contains a SecurityManager instance.  Returning.");
            return context;
        }
        log.trace("No SecurityManager found in context.  Adding self reference.");
        context.setSecurityManager(this);
        return context;
    }

    /**
     * Attempts to resolve any associated session based on the context and returns a
     * context that represents this resolved {@code Session} to ensure it may be referenced if necessary by the
     * invoked {@link SubjectFactory} that performs actual {@link Subject} construction.
     * <p/>
     * If there is a {@code Session} already in the context because that is what the caller wants to be used for
     * {@code Subject} construction, or if no session is resolved, this method effectively does nothing
     * returns the context method argument unaltered.
     *
     * @param context the subject context data that may resolve a Session instance.
     * @return The context to use to pass to a {@link SubjectFactory} for subject creation.
     * @since 1.0
     */
    private SubjectContext resolveSession(SubjectContext context) {
        if (context.resolveSession() != null) {
            log.debug("Context already contains a session.  Returning.");
            return context;
        }
        try {

            //Context couldn't resolve it directly,
            // let's see if we can since we have direct access to
            // the session manager:
            IOTClient session = resolveContextSession(context);

            if (session != null) {

                context.setAuthenticated(true);

                context.setSession(session);

                PrincipalCollection principles = session.getPrincipleCollection();
                if(null != principles){
                    context.setPrincipals(principles);
                }

            }
        } catch (InvalidSessionException e) {
            log.trace("Resolved SubjectContext context session is invalid.  Ignoring and creating an anonymous " +
                    "(session-less) Subject instance.", e);
        }
        return context;
    }

    protected IOTClient resolveContextSession( SubjectContext context) throws InvalidSessionException {

        Serializable sessionId = context.getSessionId();

        if (sessionId != null) {
            SessionKey key = new DefaultSessionKey(sessionId);
            return (IOTClient) getSession(key);
        }

        return null;

    }


    protected void stopSession(Subject subject) {
        Session s = subject.getSession(false);
        if (s != null) {
           s.stop();
        }


    }
}
