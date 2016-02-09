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

package com.caricah.iotracah.bootstrap.security.realm;

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTSession;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.shiro.ShiroException;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.eis.SessionDAO;
import org.apache.shiro.session.mgt.eis.SessionIdGenerator;
import org.apache.shiro.util.Destroyable;
import org.apache.shiro.util.Initializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.cache.Cache;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 6/15/15
 */
public class IOTSessionDAO implements SessionDAO, Initializable, Destroyable{

    private static final Logger log = LoggerFactory.getLogger(IOTSessionDAO.class);
    private final IgniteCache<Serializable, IOTSession> sessionsCache;

    public IOTSessionDAO(IgniteCache<Serializable, IOTSession> sessionsCache, IgniteAtomicSequence igniteAtomicSequence){
        this.sessionsCache = sessionsCache;
    }

    public IgniteCache<Serializable, IOTSession> getSessionsCache() {
        return sessionsCache;
    }

    /**
     * Initializes this object.
     *
     * @throws ShiroException if an exception occurs during initialization.
     */
    @Override
    public void init() throws ShiroException {

    }


    /**
     * Called when this object is being destroyed, allowing any necessary cleanup of internal resources.
     *
     * @throws Exception if an exception occurs during object destruction.
     */
    @Override
    public void destroy() throws Exception {

        getSessionsCache().close();

    }


    protected Session storeSession(Serializable id, Session session) {
         getSessionsCache().put(id, (IOTSession) session);
        return  session;
    }




    /**
     * Subclass hook to actually persist the given <tt>Session</tt> instance to the underlying EIS.
     *
     * @param session the Session instance to persist to the EIS.
     * @return the id of the session created in the EIS (i.e. this is almost always a primary key and should be the
     * value returned from {@link Session#getId() Session.getId()}.
     */
    protected Serializable doCreate(Session session) {

        try {
            Serializable sessionId = ((IOTSession) session).generateIdKey();

            assignSessionId(session, sessionId);
            storeSession(sessionId, session);
            return sessionId;

        } catch (UnRetriableException e) {
            throw new RuntimeException(e);
        }




    }

    /**
     * Subclass implementation hook that retrieves the Session object from the underlying EIS or {@code null} if a
     * session with that ID could not be found.
     *
     * @param sessionId the id of the <tt>Session</tt> to retrieve.
     * @return the Session in the EIS identified by <tt>sessionId</tt> or {@code null} if a
     * session with that ID could not be found.
     */
    protected Session doReadSession(Serializable sessionId) {

        return getSessionsCache().get(sessionId);
    }

    /**
     * Inserts a new Session record into the underling EIS (e.g. Relational database, file system, persistent cache,
     * etc, depending on the DAO implementation).
     * <p>
     * After this method is invoked, the {@link Session#getId()}
     * method executed on the argument must return a valid session identifier.  That is, the following should
     * always be true:
     * <pre>
     * Serializable id = create( session );
     * id.equals( session.getId() ) == true</pre>
     *
     * Implementations are free to throw any exceptions that might occur due to
     * integrity violation constraints or other EIS related errors.
     *
     * @param session the {@link Session} object to create in the EIS.
     * @return the EIS id (e.g. primary key) of the created {@code Session} object.
     */
    @Override
    public Serializable create(Session session) {
        Serializable sessionId = doCreate(session);
        verifySessionId(sessionId);
        return sessionId;
    }

    /**
     * Ensures the sessionId returned from the subclass implementation of {@link #doCreate} is not null and not
     * already in use.
     *
     * @param sessionId session id returned from the subclass implementation of {@link #doCreate}
     */
    private void verifySessionId(Serializable sessionId) {
        if (sessionId == null) {
            String msg = "sessionId returned from doCreate implementation is null.  Please verify the implementation.";
            throw new IllegalStateException(msg);
        }
    }

    /**
     * Utility method available to subclasses that wish to
     * assign a generated session ID to the session instance directly.  This method is not used by the
     * {@code AbstractSessionDAO} implementation directly, but it is provided so subclasses don't
     * need to know the {@code Session} implementation if they don't need to.
     * <p/>
     * This default implementation casts the argument to a {@link IOTSession}, Iotracah's default EIS implementation.
     *
     * @param session   the session instance to which the sessionId will be applied
     * @param sessionId the id to assign to the specified session instance.
     */
    protected void assignSessionId(Session session, Serializable sessionId) {
        ((IOTSession) session).setId(sessionId);
    }

    /**
     * Retrieves the session from the EIS uniquely identified by the specified
     * {@code sessionId}.
     *
     * @param sessionId the system-wide unique identifier of the Session object to retrieve from
     *                  the EIS.
     * @return the persisted session in the EIS identified by {@code sessionId}.
     * @throws UnknownSessionException if there is no EIS record for any session with the
     *                                 specified {@code sessionId}
     */
    @Override
    public Session readSession(Serializable sessionId) throws UnknownSessionException {
        Session s = doReadSession(sessionId);
        if (s == null) {
            throw new UnknownSessionException("There is no session with id [" + sessionId + "]");
        }
        return s;
    }

    /**
     * Updates (persists) data from a previously created Session instance in the EIS identified by
     * {@code {@link Session#getId() session.getId()}}.  This effectively propagates
     * the data in the argument to the EIS record previously saved.
     * <p>
     * In addition to UnknownSessionException, implementations are free to throw any other
     * exceptions that might occur due to integrity violation constraints or other EIS related
     * errors.
     *
     * @param session the Session to update
     * @throws UnknownSessionException if no existing EIS session record exists with the
     *                                                          identifier of {@link Session#getId() session.getSessionId()}
     */
    @Override
    public void update(Session session) throws UnknownSessionException {

        storeSession(session.getId(), session);
     }

     /**
     * Deletes the associated EIS record of the specified {@code session}.  If there never
     * existed a session EIS record with the identifier of
     * {@link Session#getId() session.getId()}, then this method does nothing.
     *
     * @param session the session to delete.
     */
    @Override
    public void delete(Session session) {
        if (session == null) {
            throw new NullPointerException("session argument cannot be null.");
        }
        Serializable id = session.getId();
        if (id != null) {
            getSessionsCache().remove(id);
        }
    }

    /**
     * Returns all sessions in the EIS that are considered active, meaning all sessions that
     * haven't been stopped/expired.  This is primarily used to validate potential orphans.
     * <p>
     * If there are no active sessions in the EIS, this method may return an empty collection or {@code null}.
     * <h4>Performance</h4>
     * This method should be as efficient as possible, especially in larger systems where there might be
     * thousands of active sessions.  Large scale/high performance
     * implementations will often return a subset of the total active sessions and perform validation a little more
     * frequently, rather than return a massive set and validate infrequently.  If efficient and possible, it would
     * make sense to return the oldest unstopped sessions available, ordered by
     * {@link Session#getLastAccessTime() lastAccessTime}.
     * <h4>Smart Results</h4>
     * <em>Ideally</em> this method would only return active sessions that the EIS was certain should be invalided.
     * Typically that is any session that is not stopped and where its lastAccessTimestamp is older than the session
     * timeout.
     * <p>
     * For example, if sessions were backed by a relational database or SQL-92 'query-able' enterprise cache, you might
     * return something similar to the results returned by this query (assuming
     * {@link org.apache.shiro.session.mgt.SimpleSession SimpleSession}s were being stored):
     * <pre>
     * select * from sessions s where s.lastAccessTimestamp < ? and s.stopTimestamp is null
     * </pre>
     * where the {@code ?} parameter is a date instance equal to 'now' minus the session timeout
     * (e.g. now - 30 minutes).
     *
     * @return a Collection of {@code Session}s that are considered active, or an
     * empty collection or {@code null} if there are no active sessions.
     */
    @Override
    public Collection<Session> getActiveSessions() {

        try {

            String query = "active = ? AND expiryTimestamp < ? LIMIT ?";

            Instant instant = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant();

            Object[] params = { true, instant.toEpochMilli(), 100};

            SqlQuery sql = new SqlQuery<Serializable, IOTSession>(IOTSession.class, query);
            sql.setArgs(params);

            // Find all messages belonging to a client.
            QueryCursor<Cache.Entry<Serializable, IOTSession>> queryResult = getSessionsCache().query(sql);


            // Find all messages belonging to a client.

            Set<Session> activeSessions = new HashSet<>();
            queryResult.forEach(entry -> activeSessions.add(entry.getValue()));

            return activeSessions;
        } catch (Exception e) {
            log.error(" getActiveSessions : problems with active sessions collector ", e);
            return Collections.emptySet();
        }

    }

}
