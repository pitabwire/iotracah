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

package com.caricah.iotracah.bootstrap.security.realm.state;

import com.caricah.iotracah.bootstrap.security.realm.IOTSecurityDatastore;
import com.caricah.iotracah.bootstrap.security.realm.auth.IdConstruct;
import org.apache.shiro.authc.Account;
import org.apache.shiro.authc.SaltedAuthenticationInfo;
import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.util.ByteSource;

import java.io.Serializable;
import java.util.*;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IOTAccount implements Account,  SaltedAuthenticationInfo, Serializable {


    private transient IOTSecurityDatastore iotAccountDatastore;

    private transient IdConstruct idConstruct;

    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private long id;

    /** Value for dateCreated. */
    private java.sql.Timestamp dateCreated;

    /** Value for dateModified. */
    private java.sql.Timestamp dateModified;

    /** Value for isActive. */
    private boolean isActive;

    /** Value for username. */
    private String username;

    /** Value for credential. */
    private String credential;

    /** Value for credentialSalt. */
    private Object credentialSalt;

    /** Value for rolelist. */
    private String rolelist;

    /** Value for isLocked. */
    private boolean isLocked;

    /** Value for isCredentialExpired. */
    private boolean isCredentialExpired;

    /** Value for partitionId. */
    private String partitionId;


    /**
     * Default no-argument constructor.
     */
    public IOTAccount() {
    }

    /**
     * Constructs a SimpleAccount instance for the specified realm with the given principal and credentials, with the
     * the assigned roles and permissions.
     *
     * @param idConstruct   the 'primary' identifying attributes of the account, for example, a user id or username.
     * @param credentials the credentials that verify identity for the account
     */
    public IOTAccount(IdConstruct idConstruct, String credentials) {

        this.idConstruct = idConstruct;

        this.setPartitionId(idConstruct.getPartition());
        this.setUsername(idConstruct.getUsername());
        this.setCredential(credentials);

    }

    public IOTSecurityDatastore getIotAccountDatastore() {
        return iotAccountDatastore;
    }

    public void setIotAccountDatastore(IOTSecurityDatastore iotAccountDatastore) {
        this.iotAccountDatastore = iotAccountDatastore;
    }


    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public long getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(long id) {
        this.id = id;
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
     * Gets credential.
     *
     * @return Value for credential.
     */
    public String getCredential() {
        return credential;
    }

    /**
     * Sets credential.
     *
     * @param credential New value for credential.
     */
    public void setCredential(String credential) {
        this.credential = credential;
    }

    /**
     * Gets credentialSalt.
     *
     * @return Value for credentialSalt.
     */
    public Object getCredentialSalt() {
        return credentialSalt;
    }

    /**
     * Sets credentialSalt.
     *
     * @param credentialSalt New value for credentialSalt.
     */
    public void setCredentialSalt(Object credentialSalt) {
        this.credentialSalt = credentialSalt;
    }

    /**
     * Gets rolelist.
     *
     * @return Value for rolelist.
     */
    public String getRolelist() {
        return rolelist;
    }

    /**
     * Sets rolelist.
     *
     * @param rolelist New value for rolelist.
     */
    public void setRolelist(String rolelist) {
        this.rolelist = rolelist;
    }

    /**
     * Gets isLocked.
     *
     * @return Value for isLocked.
     */
    public boolean getIsLocked() {
        return isLocked;
    }

    /**
     * Sets isLocked.
     *
     * @param isLocked New value for isLocked.
     */
    public void setIsLocked(boolean isLocked) {
        this.isLocked = isLocked;
    }

    /**
     * Gets isCredentialExpired.
     *
     * @return Value for isCredentialExpired.
     */
    public boolean getIsCredentialExpired() {
        return isCredentialExpired;
    }

    /**
     * Sets isCredentialExpired.
     *
     * @param isCredentialExpired New value for isCredentialExpired.
     */
    public void setIsCredentialExpired(boolean isCredentialExpired) {
        this.isCredentialExpired = isCredentialExpired;
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
     * Returns all principals associated with the corresponding Subject.  Each principal is an identifying piece of
     * information useful to the application such as a username, or user id, a given name, etc - anything useful
     * to the application to identify the current <code>Subject</code>.
     * <p>
     * The returned PrincipalCollection should <em>not</em> contain any credentials used to verify principals, such
     * as passwords, private keys, etc.  Those should be instead returned by {@link #getCredentials() getCredentials()}.
     *
     * @return all principals associated with the corresponding Subject.
     */
    @Override
    public PrincipalCollection getPrincipals() {

        if(Objects.isNull(idConstruct)){

            idConstruct = new IdConstruct(getPartitionId(), getUsername(), null);

        }


        return new SimplePrincipalCollection(idConstruct, "");
    }

    /**
     * Returns the credentials associated with the corresponding Subject.  A credential verifies one or more of the
     * {@link #getPrincipals() principals} associated with the Subject, such as a password or private key.  Credentials
     * are used by Shiro particularly during the authentication process to ensure that submitted credentials
     * during a login attempt match exactly the credentials here in the <code>AuthenticationInfo</code> instance.
     *
     * @return the credentials associated with the corresponding Subject.
     */
    @Override
    public Object getCredentials() {
        return getCredential();
    }

    /**
     * Returns the names of all roles assigned to a corresponding Subject.
     *
     * @return the names of all roles assigned to a corresponding Subject.
     */
    @Override
    public Collection<String> getRoles() {

        if(Objects.isNull(getRolelist())){
            return Collections.emptyList();
        }else{
            
            return Arrays.asList(getRolelist().split(","));

        }
    }

    /**
     * Returns all string-based permissions assigned to the corresponding Subject.  The permissions here plus those
     * returned from {@link #getObjectPermissions() getObjectPermissions()} represent the total set of permissions
     * assigned.  The aggregate set is used to perform a permission authorization check.
     * <p>
     * This method is a convenience mechanism that allows Realms to represent permissions as Strings if they choose.
     * When performing a security check, a <code>Realm</code> usually converts these strings to object
     * {@link Permission Permission}s via an internal
     * {@link PermissionResolver PermissionResolver}
     * in order to perform the actual permission check.  This is not a requirement of course, since <code>Realm</code>s
     * can perform security checks in whatever manner deemed necessary, but this explains the conversion mechanism that
     * most Shiro Realms execute for string-based permission checks.
     *
     * @return all string-based permissions assigned to the corresponding Subject.
     */
    @Override
    public Collection<String> getStringPermissions() {

        IdConstruct idConstruct = (IdConstruct) getPrincipals().getPrimaryPrincipal();
        HashSet<String> stringPermissions = new HashSet<>();
         getRoles().forEach(role->{
           IOTRole iotRole = getIotAccountDatastore().getIOTRole(idConstruct.getPartition(), role);
             iotRole.getPermissions().forEach(permission -> stringPermissions.add(permission.toString()));

         });
        return stringPermissions;
    }

    /**
     * Returns all type-safe {@link Permission Permission}s assigned to the corresponding Subject.  The permissions
     * returned from this method plus any returned from {@link #getStringPermissions() getStringPermissions()}
     * represent the total set of permissions.  The aggregate set is used to perform a permission authorization check.
     *
     * @return all type-safe {@link Permission Permission}s assigned to the corresponding Subject.
     */
    @Override
    public Collection<Permission> getObjectPermissions() {
        IdConstruct idConstruct = (IdConstruct) getPrincipals().getPrimaryPrincipal();
        HashSet<Permission> permissions = new HashSet<>();
        getRoles().forEach(role->{
            IOTRole iotRole = getIotAccountDatastore().getIOTRole(idConstruct.getPartition(), role);
            permissions.addAll(iotRole.getPermissions());

        });
        return permissions;
    }


    /**
     * Adds a role to this Account's set of assigned roles.  Simply delegates to
     * <code>this.authzInfo.addRole(role)</code>.
     *
     * @param role a role to assign to this Account.
     */
    public void addRole(String role) {

        String roles = getRolelist();

        if(Objects.isNull(roles) || roles.isEmpty()){

            setRolelist(role);

        }else{

            String[] roleArray = roles.split(",");
            HashSet<String> roleHashSet = new HashSet<>();
            Collections.addAll(roleHashSet, roleArray);

            roleHashSet.add(role);

            roles = String.join(",", roleHashSet);
            setRolelist(roles);


        }


    }



    /**
     * If the {@link #getPrincipals() principals} are not null, returns <code>principals.hashCode()</code>, otherwise
     * returns 0 (zero).
     *
     * @return <code>principals.hashCode()</code> if they are not null, 0 (zero) otherwise.
     */
    public int hashCode() {
        return (getPrincipals() != null ? getPrincipals().hashCode() : 0);
    }

    /**
     * Returns <code>true</code> if the specified object is also a {@link SimpleAccount SimpleAccount} and its
     * {@link #getPrincipals() principals} are equal to this object's <code>principals</code>, <code>false</code> otherwise.
     *
     * @param o the object to test for equality.
     * @return <code>true</code> if the specified object is also a {@link SimpleAccount SimpleAccount} and its
     *         {@link #getPrincipals() principals} are equal to this object's <code>principals</code>, <code>false</code> otherwise.
     */
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IOTAccount) {
            IOTAccount sa = (IOTAccount) o;

            //principal should be unique across the application, so only check this for equality:
            return Objects.equals(getPrincipals(), sa.getPrincipals());
        }
        return false;
    }

    /**
     * Returns {@link #getPrincipals() principals}.toString() if they are not null, otherwise prints out the string
     * &quot;empty&quot;
     *
     * @return the String representation of this Account object.
     */
    public String toString() {
        return getPrincipals() != null ? getPrincipals().toString() : "empty";
    }

    /**
     * Returns the salt used to salt the account's credentials or {@code null} if no salt was used.
     *
     * @return the salt used to salt the account's credentials or {@code null} if no salt was used.
     */
    @Override
    public ByteSource getCredentialsSalt() {
        return null;
    }
}
