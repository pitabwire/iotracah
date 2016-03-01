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

import com.caricah.iotracah.bootstrap.data.messages.PublishMessage;
import com.caricah.iotracah.bootstrap.data.messages.base.IOTMessage;
import com.caricah.iotracah.bootstrap.data.messages.base.Protocol;
import com.caricah.iotracah.bootstrap.data.models.client.IotClientKey;
import com.caricah.iotracah.bootstrap.exceptions.NotImplementedException;
import com.caricah.iotracah.bootstrap.security.IOTSessionManager;
import com.caricah.iotracah.bootstrap.security.realm.auth.IdConstruct;
import org.apache.shiro.session.ExpiredSessionException;
import org.apache.shiro.session.InvalidSessionException;
import org.apache.shiro.session.StoppedSessionException;
import org.apache.shiro.session.mgt.DefaultSessionKey;
import org.apache.shiro.session.mgt.ValidatingSession;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;


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
public class IOTClient implements ValidatingSession, Serializable {

    private transient static final Logger log = LoggerFactory.getLogger(IOTClient.class);

    public static final String CONTEXT_PARTITION_KEY = "_ctx_part_k";
    public static final String CONTEXT_USERNAME_KEY = "_ctx_u_id_k";
    public static final String CONTEXT_CLIENT_ID_KEY = "_ctx_cl_k";

    public static final long DEFAULT_GLOBAL_SESSION_TIMEOUT = 30 * 60;

    /**
     * Handle to the target NativeSessionManager that will support the delegate calls.
     */
    private transient IOTSessionManager sessionManager;


    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Value for dateCreated.
     */
    private java.sql.Timestamp dateCreated;

    /**
     * Value for dateModified.
     */
    private java.sql.Timestamp dateModified;

    /**
     * Value for isActive.
     */
    private boolean isActive;

    /**
     * Value for sessionId.
     */
    private String sessionId;

    /**
     * Value for username.
     */
    private String username;

    /**
     * Value for clientIdentification.
     */
    private String clientIdentification;

    /**
     * Value for isCleanSession.
     */
    private boolean isCleanSession;

    /**
     * Value for connectionId.
     */
    private String connectionId;

    /**
     * Value for connectedCluster.
     */
    private String connectedCluster;

    /**
     * Value for connectedNode.
     */
    private String connectedNode;

    /**
     * Value for protocol.
     */
    private String protocol;

    /**
     * Value for protocolData.
     */
    private String protocolData;

    /**
     * Value for authKey.
     */
    private String authKey;

    /**
     * Value for timeout.
     */
    private long timeout;

    /**
     * Value for startTimestamp.
     */
    private java.sql.Timestamp startTimestamp;

    /**
     * Value for stopTimestamp.
     */
    private java.sql.Timestamp stopTimestamp;

    /**
     * Value for lastAccessTime.
     */
    private java.sql.Timestamp lastAccessTime;

    /**
     * Value for expiryTimestamp.
     */
    private java.sql.Timestamp expiryTimestamp;

    /**
     * Value for isExpired.
     */
    private boolean isExpired;

    /**
     * Value for host.
     */
    private String host;

    /**
     * Value for attributes.
     */
    private String attributes;

    /**
     * Value for partitionId.
     */
    private String partitionId;


    public IOTClient() {
        this.setTimeout(DEFAULT_GLOBAL_SESSION_TIMEOUT);
        this.setStartTimestamp(Timestamp.from(Instant.now()));
        this.setLastAccessTime(getStartTimestamp());
        this.setExpiryTimestamp(Timestamp.from(Instant.now().plusSeconds(getTimeout())));
    }

    public IOTClient(String host) {
        this();
        this.setHost(host);
    }

    public IOTSessionManager getSessionManager() {
        return sessionManager;
    }

    public void setSessionManager(IOTSessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    /**
     * Gets dateCreated.
     *
     * @return Value for dateCreated.
     */
    public java.sql.Timestamp getDateCreated() {
        return dateCreated;
    }

    /**
     * Sets dateCreated.
     *
     * @param dateCreated New value for dateCreated.
     */
    public void setDateCreated(java.sql.Timestamp dateCreated) {
        this.dateCreated = dateCreated;
    }

    /**
     * Gets dateModified.
     *
     * @return Value for dateModified.
     */
    public java.sql.Timestamp getDateModified() {
        return dateModified;
    }

    /**
     * Sets dateModified.
     *
     * @param dateModified New value for dateModified.
     */
    public void setDateModified(java.sql.Timestamp dateModified) {
        this.dateModified = dateModified;
    }

    /**
     * Gets isActive.
     *
     * @return Value for isActive.
     */
    public boolean getIsActive() {
        return isActive;
    }

    /**
     * Sets isActive.
     *
     * @param isActive New value for isActive.
     */
    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    /**
     * Gets sessionId.
     *
     * @return Value for sessionId.
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Sets sessionId.
     *
     * @param sessionId New value for sessionId.
     */
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * Gets username.
     *
     * @return Value for username.
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets username.
     *
     * @param username New value for username.
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets clientIdentification.
     *
     * @return Value for clientIdentification.
     */
    public String getClientIdentification() {
        return clientIdentification;
    }

    /**
     * Sets clientIdentification.
     *
     * @param clientIdentification New value for clientIdentification.
     */
    public void setClientIdentification(String clientIdentification) {
        this.clientIdentification = clientIdentification;
    }

    /**
     * Gets isCleanSession.
     *
     * @return Value for isCleanSession.
     */
    public boolean getIsCleanSession() {
        return isCleanSession;
    }

    /**
     * Sets isCleanSession.
     *
     * @param isCleanSession New value for isCleanSession.
     */
    public void setIsCleanSession(boolean isCleanSession) {
        this.isCleanSession = isCleanSession;
    }

    /**
     * Gets connectionId.
     *
     * @return Value for connectionId.
     */
    public String getConnectionId() {
        return connectionId;
    }

    /**
     * Sets connectionId.
     *
     * @param connectionId New value for connectionId.
     */
    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * Gets connectedCluster.
     *
     * @return Value for connectedCluster.
     */
    public String getConnectedCluster() {
        return connectedCluster;
    }

    /**
     * Sets connectedCluster.
     *
     * @param connectedCluster New value for connectedCluster.
     */
    public void setConnectedCluster(String connectedCluster) {
        this.connectedCluster = connectedCluster;
    }

    /**
     * Gets connectedNode.
     *
     * @return Value for connectedNode.
     */
    public String getConnectedNode() {
        return connectedNode;
    }

    /**
     * Sets connectedNode.
     *
     * @param connectedNode New value for connectedNode.
     */
    public void setConnectedNode(String connectedNode) {
        this.connectedNode = connectedNode;
    }

    /**
     * Gets protocol.
     *
     * @return Value for protocol.
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets protocol.
     *
     * @param protocol New value for protocol.
     */
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    /**
     * Gets protocolData.
     *
     * @return Value for protocolData.
     */
    public String getProtocolData() {
        return protocolData;
    }

    /**
     * Sets protocolData.
     *
     * @param protocolData New value for protocolData.
     */
    public void setProtocolData(String protocolData) {
        this.protocolData = protocolData;
    }

    /**
     * Gets authKey.
     *
     * @return Value for authKey.
     */
    public String getAuthKey() {
        return authKey;
    }

    /**
     * Sets authKey.
     *
     * @param authKey New value for authKey.
     */
    public void setAuthKey(String authKey) {
        this.authKey = authKey;
    }

    /**
     * Gets timeout.
     *
     * @return Value for timeout.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets timeout.
     *
     * @param timeout New value for timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Gets startTimestamp.
     *
     * @return Value for startTimestamp.
     */
    public java.sql.Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Sets startTimestamp.
     *
     * @param startTimestamp New value for startTimestamp.
     */
    public void setStartTimestamp(java.sql.Timestamp startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    /**
     * Gets stopTimestamp.
     *
     * @return Value for stopTimestamp.
     */
    public java.sql.Timestamp getStopTimestamp() {
        return stopTimestamp;
    }

    /**
     * Sets stopTimestamp.
     *
     * @param stopTimestamp New value for stopTimestamp.
     */
    public void setStopTimestamp(java.sql.Timestamp stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }

    /**
     * Gets lastAccessTime.
     *
     * @return Value for lastAccessTime.
     */
    public java.sql.Timestamp getLastAccessTime() {
        return lastAccessTime;
    }

    /**
     * Sets lastAccessTime.
     *
     * @param lastAccessTime New value for lastAccessTime.
     */
    public void setLastAccessTime(java.sql.Timestamp lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    /**
     * Gets expiryTimestamp.
     *
     * @return Value for expiryTimestamp.
     */
    public java.sql.Timestamp getExpiryTimestamp() {
        return expiryTimestamp;
    }

    /**
     * Sets expiryTimestamp.
     *
     * @param expiryTimestamp New value for expiryTimestamp.
     */
    public void setExpiryTimestamp(java.sql.Timestamp expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
    }

    /**
     * Gets isExpired.
     *
     * @return Value for isExpired.
     */
    public boolean getIsExpired() {
        return isExpired;
    }

    /**
     * Sets isExpired.
     *
     * @param isExpired New value for isExpired.
     */
    public void setIsExpired(boolean isExpired) {
        this.isExpired = isExpired;
    }

    /**
     * Gets host.
     *
     * @return Value for host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets host.
     *
     * @param host New value for host.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Gets attributes.
     *
     * @return Value for attributes.
     */
    public String getAttributes() {
        return attributes;
    }

    /**
     * Sets attributes.
     *
     * @param attributes New value for attributes.
     */
    public void setAttributes(String attributes) {
        this.attributes = attributes;
    }

    /**
     * Gets partitionId.
     *
     * @return Value for partitionId.
     */
    public String getPartitionId() {
        return partitionId;
    }

    /**
     * Sets partitionId.
     *
     * @param partitionId New value for partitionId.
     */
    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IOTClient))
            return false;

        IOTClient that = (IOTClient) o;

        if (dateCreated != null ? !dateCreated.equals(that.dateCreated) : that.dateCreated != null)
            return false;

        if (dateModified != null ? !dateModified.equals(that.dateModified) : that.dateModified != null)
            return false;

        if (isActive != that.isActive)
            return false;

        if (sessionId != null ? !sessionId.equals(that.sessionId) : that.sessionId != null)
            return false;

        if (username != null ? !username.equals(that.username) : that.username != null)
            return false;

        if (clientIdentification != null ? !clientIdentification.equals(that.clientIdentification) : that.clientIdentification != null)
            return false;

        if (isCleanSession != that.isCleanSession)
            return false;

        if (connectionId != null ? !connectionId.equals(that.connectionId) : that.connectionId != null)
            return false;

        if (connectedCluster != null ? !connectedCluster.equals(that.connectedCluster) : that.connectedCluster != null)
            return false;

        if (connectedNode != null ? !connectedNode.equals(that.connectedNode) : that.connectedNode != null)
            return false;

        if (protocol != null ? !protocol.equals(that.protocol) : that.protocol != null)
            return false;

        if (protocolData != null ? !protocolData.equals(that.protocolData) : that.protocolData != null)
            return false;

        if (authKey != null ? !authKey.equals(that.authKey) : that.authKey != null)
            return false;

        if (timeout != that.timeout)
            return false;

        if (startTimestamp != null ? !startTimestamp.equals(that.startTimestamp) : that.startTimestamp != null)
            return false;

        if (stopTimestamp != null ? !stopTimestamp.equals(that.stopTimestamp) : that.stopTimestamp != null)
            return false;

        if (lastAccessTime != null ? !lastAccessTime.equals(that.lastAccessTime) : that.lastAccessTime != null)
            return false;

        if (expiryTimestamp != null ? !expiryTimestamp.equals(that.expiryTimestamp) : that.expiryTimestamp != null)
            return false;

        if (isExpired != that.isExpired)
            return false;

        if (host != null ? !host.equals(that.host) : that.host != null)
            return false;

        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null)
            return false;

        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null)
            return false;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        int res = dateCreated != null ? dateCreated.hashCode() : 0;

        res = 31 * res + (dateModified != null ? dateModified.hashCode() : 0);

        res = 31 * res + (isActive ? 1 : 0);

        res = 31 * res + (sessionId != null ? sessionId.hashCode() : 0);

        res = 31 * res + (username != null ? username.hashCode() : 0);

        res = 31 * res + (clientIdentification != null ? clientIdentification.hashCode() : 0);

        res = 31 * res + (isCleanSession ? 1 : 0);

        res = 31 * res + (connectionId != null ? connectionId.hashCode() : 0);

        res = 31 * res + (connectedCluster != null ? connectedCluster.hashCode() : 0);

        res = 31 * res + (connectedNode != null ? connectedNode.hashCode() : 0);

        res = 31 * res + (protocol != null ? protocol.hashCode() : 0);

        res = 31 * res + (protocolData != null ? protocolData.hashCode() : 0);

        res = 31 * res + (authKey != null ? authKey.hashCode() : 0);

        res = 31 * res + (int) (timeout ^ (timeout >>> 32));

        res = 31 * res + (startTimestamp != null ? startTimestamp.hashCode() : 0);

        res = 31 * res + (stopTimestamp != null ? stopTimestamp.hashCode() : 0);

        res = 31 * res + (lastAccessTime != null ? lastAccessTime.hashCode() : 0);

        res = 31 * res + (expiryTimestamp != null ? expiryTimestamp.hashCode() : 0);

        res = 31 * res + (isExpired ? 1 : 0);

        res = 31 * res + (host != null ? host.hashCode() : 0);

        res = 31 * res + (attributes != null ? attributes.hashCode() : 0);

        res = 31 * res + (partitionId != null ? partitionId.hashCode() : 0);

        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "IotClient [dateCreated=" + dateCreated +
                ", dateModified=" + dateModified +
                ", isActive=" + isActive +
                ", sessionId=" + sessionId +
                ", username=" + username +
                ", clientIdentification=" + clientIdentification +
                ", isCleanSession=" + isCleanSession +
                ", connectionId=" + connectionId +
                ", connectedCluster=" + connectedCluster +
                ", connectedNode=" + connectedNode +
                ", protocol=" + protocol +
                ", protocolData=" + protocolData +
                ", authKey=" + authKey +
                ", timeout=" + timeout +
                ", startTimestamp=" + startTimestamp +
                ", stopTimestamp=" + stopTimestamp +
                ", lastAccessTime=" + lastAccessTime +
                ", expiryTimestamp=" + expiryTimestamp +
                ", isExpired=" + isExpired +
                ", host=" + host +
                ", attributes=" + attributes +
                ", partitionId=" + partitionId +
                "]";
    }


    /**
     * Returns the unique identifier assigned by the system upon session creation.
     * <p>
     * All return values from this method are expected to have proper {@code toString()},
     * {@code equals()}, and {@code hashCode()} implementations. Good candidates for such
     * an identifier are {@link UUID UUID}s, {@link Integer Integer}s, and
     * {@link String String}s.
     *
     * @return The unique identifier assigned to the session upon creation.
     */
    @Override
    public Serializable getId() {

        IotClientKey clientKey = new IotClientKey();

        if (Objects.isNull(getSessionId()) || getSessionId().isEmpty()) {

            return null;
        }

        clientKey.setSessionId(getSessionId());

        return clientKey;
    }

    public void touch() {
        setLastAccessTime(Timestamp.from(Instant.now()));

        setExpiryTimestamp(Timestamp.from(Instant.now().plusSeconds(getTimeout())));

        getSessionManager().getSessionDAO().update(this);
    }

    public void stop() {
        if (getStopTimestamp() == null) {
            setStopTimestamp(Timestamp.from(Instant.now()));
        }

        setIsActive(false);

        touch();

        getSessionManager().stop(new DefaultSessionKey(getId()));

    }

    protected boolean isStopped() {


        return getStopTimestamp() != null;
    }

    protected void expire() {
        setIsExpired(true);
        stop();
    }

    /**
     * @since 0.9
     */
    public boolean isValid() {
        return !isStopped() && !getIsExpired();
    }

    /**
     * Determines if this session is expired.
     *
     * @return true if the specified session has expired, false otherwise.
     */
    protected boolean isTimedOut() {

        if (getIsExpired()) {
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


            Instant expiryTime = Instant.now().minusSeconds(timeout);

            return lastAccessTime.before(Date.from(expiryTime));
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
        if (getIsActive() && isStopped()) {
            //timestamp is set, so the session is considered stopped:
            String msg = "Session with id [" + getId() + "] has been " +
                    "explicitly stopped.  No further interaction under this session is " +
                    "allowed.";
            throw new StoppedSessionException(msg);
        }

        //check for expiration
        if (getIsActive() && isTimedOut()) {
            expire();

            //throw an exception explaining details of why it expired:
            Date lastAccessTime = getLastAccessTime();
            long timeout = getTimeout();

            Serializable sessionId = getId();

            DateFormat df = DateFormat.getInstance();
            String msg = "Session with id [" + sessionId + "] has expired. " +
                    "Last access time: " + df.format(lastAccessTime) +
                    ".  Current time: " + df.format(new Date()) +
                    ".  Session timeout is set to " + timeout + " seconds (" +
                    (timeout * 1d) / 60 + " minutes)";
            if (log.isTraceEnabled()) {
                log.trace(msg);
            }
            throw new ExpiredSessionException(msg);
        }
    }

    public Collection<Object> getAttributeKeys() throws InvalidSessionException {
        throw new NotImplementedException();
    }

    public Object getAttribute(Object key) {
        throw new NotImplementedException();
    }

    public void setAttribute(Object key, Object value) {
        throw new NotImplementedException();
    }

    public Object removeAttribute(Object key) {
        throw new NotImplementedException();
    }


    /**
     * Session transmission of appropriate propagation metadata.
     *
     * @param iotMessage
     * @param <T>
     * @return
     */
    public <T extends IOTMessage> T copyTransmissionData(T iotMessage) {

        iotMessage.setSessionId(getSessionId());
        iotMessage.setProtocol(Protocol.fromString(getProtocol()));
        iotMessage.setConnectionId(getConnectionId());
        iotMessage.setNodeId(UUID.fromString(getConnectedNode()));
        iotMessage.setCluster(getConnectedCluster());

        if (iotMessage instanceof PublishMessage) {
            PublishMessage publishMessage = (PublishMessage) iotMessage;
            publishMessage.setPartitionId(getPartitionId());
            publishMessage.setClientId(getSessionId());
            publishMessage.setProtocolData(getProtocolData());

            return (T) publishMessage;
        } else {
            return iotMessage;
        }
    }


    public static IotClientKey keyFromStrings(String partitionId, String clientIdentification) {
        IotClientKey sessionId = new IotClientKey();
        sessionId.setSessionId("p[" + partitionId + "]" + clientIdentification);
        return sessionId;
    }


    public PrincipalCollection getPrincipleCollection() {
        IdConstruct idConstruct = new IdConstruct(getPartitionId(), getUsername(), getClientIdentification());
        return new SimplePrincipalCollection(idConstruct, "");
    }
}

