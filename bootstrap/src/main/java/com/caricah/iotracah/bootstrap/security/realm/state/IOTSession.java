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

package com.caricah.iotracah.bootstrap.security.realm.state;

import com.caricah.iotracah.bootstrap.data.IdKeyComposer;
import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.bootstrap.data.messages.base.Protocol;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.IOTSessionManager;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.shiro.session.ExpiredSessionException;
import org.apache.shiro.session.InvalidSessionException;
import org.apache.shiro.session.StoppedSessionException;
import org.apache.shiro.session.mgt.AbstractNativeSessionManager;
import org.apache.shiro.session.mgt.DefaultSessionKey;
import org.apache.shiro.session.mgt.NativeSessionManager;
import org.apache.shiro.session.mgt.ValidatingSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DateFormat;
import java.util.*;


/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 1/16/16
 */

/**
 * Simple {@link org.apache.shiro.session.Session} JavaBeans-compatible POJO implementation, intended to be used on the
 * business/server tier.
 *
 * @since 0.1
 */
public class IOTSession implements IdKeyComposer, ValidatingSession, Externalizable {

    private transient static final Logger log = LoggerFactory.getLogger(IOTSession.class);

    public static final String CONTEXT_PARTITION_KEY = "_ctx_part_k";
    public static final String CONTEXT_USERNAME_KEY = "_ctx_unm_k";
    public static final String CONTEXT_CLIENT_ID_KEY = "_ctx_cl_k";
    protected static final long MILLIS_PER_SECOND = 1000;
    protected static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
    public static final long DEFAULT_GLOBAL_SESSION_TIMEOUT = 30 * MILLIS_PER_MINUTE;

    /**
     * Handle to the target NativeSessionManager that will support the delegate calls.
     */
    private transient IOTSessionManager sessionManager;

    private Serializable id;
    private Date startTimestamp;
    private Date stopTimestamp;
    private Date lastAccessTime;

    @QuerySqlField(index = true)
    private long expiryTimestamp;
    private long timeout;
    private boolean expired;
    private String host;
    private Map<Object, Object> attributes;


    /**
     * =======================================================================================
     */

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(
            name = "partition_clientid_idx", order = 0)})
    private String partition;

    private String username;

    @QuerySqlField(orderedGroups = {@QuerySqlField.Group(
            name = "partition_clientid_idx", order = 2)})
    private String clientId;

    @QuerySqlField()
    private String connectedCluster;

    @QuerySqlField()
    private UUID connectedNode;

    @QuerySqlField()
    private Serializable connectionId;

    @QuerySqlField()
    private boolean active;

    private boolean cleanSession;

    private Protocol protocol;

    private String protocalData;


    /**
     * =======================================================================================
     */


    public IOTSession() {
        this.timeout = DEFAULT_GLOBAL_SESSION_TIMEOUT;
        this.startTimestamp = new Date();
        this.lastAccessTime = this.startTimestamp;
    }

    public IOTSession(String host) {
        this();
        this.host = host;
    }

    public IOTSessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(IOTSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    public Serializable getId() {
        return this.id;
    }

    public void setId(Serializable id) {
        this.id = id;
    }

    public Date getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(Date startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    /**
     * Returns the time the session was stopped, or <tt>null</tt> if the session is still active.
     * <p>
     * A session may become stopped under a number of conditions:
     * <ul>
     * <li>If the user logs out of the system, their current session is terminated (released).</li>
     * <li>If the session expires</li>
     * <li>The application explicitly calls {@link #stop()}</li>
     * <li>If there is an internal system error and the session state can no longer accurately
     * reflect the user's behavior, such in the case of a system crash</li>
     * </ul>
     * <p>
     * Once stopped, a session may no longer be used.  It is locked from all further activity.
     *
     * @return The time the session was stopped, or <tt>null</tt> if the session is still
     * active.
     */
    public Date getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(Date stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }

    public Date getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(Date lastAccessTime) {

        this.lastAccessTime = lastAccessTime;

    }

    public long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    public void setExpiryTimestamp(long expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * Returns true if this session has expired, false otherwise.  If the session has
     * expired, no further user interaction with the system may be done under this session.
     *
     * @return true if this session has expired, false otherwise.
     */
    public boolean isExpired() {
        return expired;
    }

    public void setExpired(boolean expired) {
        this.expired = expired;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Map<Object, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<Object, Object> attributes) {
        this.attributes = attributes;
    }

    /**
     * ============================================================================
     */

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public UUID getConnectedNode() {
        return connectedNode;
    }

    public void setConnectedNode(UUID connectedNode) {
        this.connectedNode = connectedNode;
    }

    public String getConnectedCluster() {
        return connectedCluster;
    }

    public void setConnectedCluster(String connectedCluster) {
        this.connectedCluster = connectedCluster;
    }

    public Serializable getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(Serializable connectionId) {
        this.connectionId = connectionId;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public String getProtocalData() {
        return protocalData;
    }

    public void setProtocalData(String protocalData) {
        this.protocalData = protocalData;
    }


    /**
     * ============================================================================
     */
    public void touch() {
        setLastAccessTime(new Date());

        setExpiryTimestamp(lastAccessTime.getTime() + getTimeout());

        getSessionManager().getSessionDAO().update(this);
    }

    public void stop() {
        if (this.stopTimestamp == null) {
            this.stopTimestamp = new Date();
        }

        setActive(false);

        touch();

        getSessionManager().stop(new DefaultSessionKey(getId()));

    }

    protected boolean isStopped() {


        return getStopTimestamp() != null ;
    }

    protected void expire() {
        this.expired = true;

        stop();
    }

    /**
     * @since 0.9
     */
    public boolean isValid() {
        return !isStopped() && !isExpired();
    }

    /**
     * Determines if this session is expired.
     *
     * @return true if the specified session has expired, false otherwise.
     */
    protected boolean isTimedOut() {

        if (isExpired()) {
            return true;
        }

        long timeout = getTimeout();

        if (timeout >= 0l) {

            Date lastAccessTime = getLastAccessTime();

            if (lastAccessTime == null) {
                String msg = "session.lastAccessTime for session with id [" +
                        getId() + "] is null.  This value must be set at " +
                        "least once, preferably at least upon instantiation.  Please check the " +
                        getClass().getName() + " implementation and ensure " +
                        "this value will be set (perhaps in the constructor?)";
                throw new IllegalStateException(msg);
            }

            // Calculate at what time a session would have been last accessed
            // for it to be expired at this point.  In other words, subtract
            // from the current time the amount of time that a session can
            // be inactive before expiring.  If the session was last accessed
            // before this time, it is expired.
            long expireTimeMillis = System.currentTimeMillis() - timeout;
            Date expireTime = new Date(expireTimeMillis);
            return lastAccessTime.before(expireTime);
        } else {
            if (log.isTraceEnabled()) {
                log.trace("No timeout for session with id [" + getId() +
                        "].  Session is not considered expired.");
            }
        }

        return false;
    }

    public void validate() throws InvalidSessionException {
        //check for stopped:
        if (isActive() && isStopped()) {
            //timestamp is set, so the session is considered stopped:
            String msg = "Session with id [" + getId() + "] has been " +
                    "explicitly stopped.  No further interaction under this session is " +
                    "allowed.";
            throw new StoppedSessionException(msg);
        }

        //check for expiration
        if (isActive() && isTimedOut()) {
            expire();

            //throw an exception explaining details of why it expired:
            Date lastAccessTime = getLastAccessTime();
            long timeout = getTimeout();

            Serializable sessionId = getId();

            DateFormat df = DateFormat.getInstance();
            String msg = "Session with id [" + sessionId + "] has expired. " +
                    "Last access time: " + df.format(lastAccessTime) +
                    ".  Current time: " + df.format(new Date()) +
                    ".  Session timeout is set to " + timeout / MILLIS_PER_SECOND + " seconds (" +
                    timeout / MILLIS_PER_MINUTE + " minutes)";
            if (log.isTraceEnabled()) {
                log.trace(msg);
            }
            throw new ExpiredSessionException(msg);
        }
    }

    private Map<Object, Object> getAttributesLazy() {
        Map<Object, Object> attributes = getAttributes();
        if (attributes == null) {
            attributes = new HashMap<>();
            setAttributes(attributes);
        }
        return attributes;
    }

    public Collection<Object> getAttributeKeys() throws InvalidSessionException {
        Map<Object, Object> attributes = getAttributes();
        if (attributes == null) {
            return Collections.emptySet();
        }
        return attributes.keySet();
    }

    public Object getAttribute(Object key) {
        Map<Object, Object> attributes = getAttributes();
        if (attributes == null) {
            return null;
        }
        return attributes.get(key);
    }

    public void setAttribute(Object key, Object value) {
        if (value == null) {
            removeAttribute(key);
        } else {
            getAttributesLazy().put(key, value);
        }
    }

    public Object removeAttribute(Object key) {
        Map<Object, Object> attributes = getAttributes();
        if (attributes == null) {
            return null;
        } else {
            return attributes.remove(key);
        }
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException{

        if(Objects.isNull(getClientId())){
            throw new UnRetriableException(" Client Id has to be non null");
        }

        return IOTSession.createIdKey(getPartition(), getClientId());

    }

    public static Serializable createIdKey(String partition, String clientId) {

        return "p["+ partition +"]"+ clientId;
    }



    /**
     * Returns {@code true} if the specified argument is an {@code instanceof} {@code SimpleSession} and both
     * {@link #getId() id}s are equal.  If the argument is a {@code SimpleSession} and either 'this' or the argument
     * does not yet have an ID assigned, the value of {@link #onEquals(IOTSession) onEquals} is returned, which
     * does a necessary attribute-based comparison when IDs are not available.
     * <p>
     * Do your best to ensure {@code SimpleSession} instances receive an ID very early in their lifecycle to
     * avoid the more expensive attributes-based comparison.
     *
     * @param obj the object to compare with this one for equality.
     * @return {@code true} if this object is equivalent to the specified argument, {@code false} otherwise.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof IOTSession) {
            IOTSession other = (IOTSession) obj;
            Serializable thisId = getId();
            Serializable otherId = other.getId();
            if (thisId != null && otherId != null) {
                return thisId.equals(otherId);
            } else {
                //fall back to an attribute based comparison:
                return onEquals(other);
            }
        }
        return false;
    }

    /**
     * Provides an attribute-based comparison (no ID comparison) - incurred <em>only</em> when 'this' or the
     * session object being compared for equality do not have a session id.
     *
     * @param ss the SimpleSession instance to compare for equality.
     * @return true if all the attributes, except the id, are equal to this object's attributes.
     * @since 1.0
     */
    protected boolean onEquals(IOTSession ss) {
        return (getStartTimestamp() != null ? getStartTimestamp().equals(ss.getStartTimestamp()) : ss.getStartTimestamp() == null) &&
                (getStopTimestamp() != null ? getStopTimestamp().equals(ss.getStopTimestamp()) : ss.getStopTimestamp() == null) &&
                (getLastAccessTime() != null ? getLastAccessTime().equals(ss.getLastAccessTime()) : ss.getLastAccessTime() == null) &&
                (getTimeout() == ss.getTimeout()) &&
                (isExpired() == ss.isExpired()) &&
                (getHost() != null ? getHost().equals(ss.getHost()) : ss.getHost() == null) &&
                (getAttributes() != null ? getAttributes().equals(ss.getAttributes()) : ss.getAttributes() == null);
    }

    /**
     * Returns the hashCode.  If the {@link #getId() id} is not {@code null}, its hashcode is returned immediately.
     * If it is {@code null}, an attributes-based hashCode will be calculated and returned.
     * <p>
     * Do your best to ensure {@code SimpleSession} instances receive an ID very early in their lifecycle to
     * avoid the more expensive attributes-based calculation.
     *
     * @return this object's hashCode
     * @since 1.0
     */
    @Override
    public int hashCode() {
        Serializable id = getId();
        if (id != null) {
            return id.hashCode();
        }
        int hashCode = getStartTimestamp() != null ? getStartTimestamp().hashCode() : 0;
        hashCode = 31 * hashCode + (getStopTimestamp() != null ? getStopTimestamp().hashCode() : 0);
        hashCode = 31 * hashCode + (getLastAccessTime() != null ? getLastAccessTime().hashCode() : 0);
        hashCode = 31 * hashCode + Long.valueOf(Math.max(getTimeout(), 0)).hashCode();
        hashCode = 31 * hashCode + Boolean.valueOf(isExpired()).hashCode();
        hashCode = 31 * hashCode + (getHost() != null ? getHost().hashCode() : 0);
        hashCode = 31 * hashCode + (getAttributes() != null ? getAttributes().hashCode() : 0);
        return hashCode;
    }

    /**
     * Session transmission of appropriate propagation metadata.
     *
     * @param iotMessage
     * @param <T>
     * @return
     */
    public <T extends IOTMessage> T copyTransmissionData(T iotMessage) {

        iotMessage.setSessionId((String) getId());
        iotMessage.setProtocol(getProtocol());
        iotMessage.setConnectionId(getConnectionId());
        iotMessage.setNodeId(getConnectedNode());
        iotMessage.setCluster(getConnectedCluster());

        if (iotMessage instanceof PublishMessage) {
            PublishMessage publishMessage = (PublishMessage) iotMessage;
            publishMessage.setPartition(getPartition());
            publishMessage.setProtocalData(getProtocalData());

        }


        return iotMessage;
    }

    /**
     * Returns the string representation of this IOTSession, equal to
     * <code>getClass().getName() + &quot;,id=&quot; + getId()</code>.
     *
     * @return the string representation of this IOTSession, equal to
     * <code>getClass().getName() + &quot;,id=&quot; + getId()</code>.
     * @since 1.0
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + ",id=" + getId();
    }

    /**
     * Serializes this object to the specified output stream for JDK Serialization.
     *
     * @param out output stream used for Object serialization.
     * @throws IOException if any of this object's fields cannot be written to the stream.
     * @since 1.0
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeObject(id);
        out.writeObject(startTimestamp);
        out.writeObject(stopTimestamp);
        out.writeObject(lastAccessTime);
        out.writeLong(expiryTimestamp);
        out.writeLong(timeout);
        out.writeBoolean(expired);
        out.writeObject(host);
        out.writeObject(attributes);


        //Properties related to client id
        out.writeObject(getClientId());
        out.writeObject(getConnectedCluster());
        out.writeObject(getConnectedNode());
        out.writeObject(getConnectionId());
        out.writeObject(getPartition());
        out.writeObject(getUsername());
        out.writeObject(getProtocol());
        out.writeObject(getProtocalData());
        out.writeBoolean(isActive());
        out.writeBoolean(isCleanSession());

    }

    /**
     * Reconstitutes this object based on the specified InputStream for JDK Serialization.
     *
     * @param in the input stream to use for reading data to populate this object.
     * @throws IOException            if the input stream cannot be used.
     * @throws ClassNotFoundException if a required class needed for instantiation is not available in the present JVM
     * @since 1.0
     */
    @SuppressWarnings({"unchecked"})
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        this.id = (Serializable) in.readObject();
        this.startTimestamp = (Date) in.readObject();
        this.stopTimestamp = (Date) in.readObject();
        this.lastAccessTime = (Date) in.readObject();
        this.expiryTimestamp = in.readLong();
        this.timeout = in.readLong();
        this.expired = in.readBoolean();
        this.host = (String) in.readObject();
        this.attributes = (Map<Object, Object>) in.readObject();


        //Properties related to client details.

        setClientId((String) in.readObject());
        setConnectedCluster((String) in.readObject());
        setConnectedNode((UUID) in.readObject());
        setConnectionId((Serializable) in.readObject());
        setPartition((String) in.readObject());
        setUsername((String) in.readObject());
        setProtocol((Protocol) in.readObject());
        setProtocalData((String) in.readObject());
        setActive(in.readBoolean());
        setCleanSession(in.readBoolean());
    }


}

