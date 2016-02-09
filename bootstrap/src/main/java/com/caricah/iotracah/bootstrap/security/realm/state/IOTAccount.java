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

import com.caricah.iotracah.bootstrap.data.IdKeyComposer;
import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import com.caricah.iotracah.bootstrap.security.realm.IOTAccountDatastore;
import com.caricah.iotracah.bootstrap.security.realm.auth.IdConstruct;
import org.apache.shiro.authc.*;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.util.ByteSource;

import java.io.*;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IOTAccount implements Account,  SaltedAuthenticationInfo, IdKeyComposer, Externalizable {


    /*--------------------------------------------
    |    I N S T A N C E   V A R I A B L E S    |
    ============================================*/
    /**
     * The internal roles collection.
     */
    protected Set<String> roles;

    /**
     * The principals identifying the account associated with this AuthenticationInfo instance.
     */
    protected PrincipalCollection principals;

    /**
     * The credentials verifying the account principals.
     */
    protected Object credentials;

    /**
     * Any salt used in hashing the credentials.
     *
     * @since 1.1
     */
    protected ByteSource credentialsSalt;

    /**
     * Indicates this account is locked.  This isn't honored by all <tt>Realms</tt> but is honored by
     * {@link org.apache.shiro.realm.SimpleAccountRealm}.
     */
    private boolean locked;

    /**
     * Indicates credentials on this account are expired.  This isn't honored by all <tt>Realms</tt> but is honored by
     * {@link org.apache.shiro.realm.SimpleAccountRealm}.
     */
    private boolean credentialsExpired;


    private IOTAccountDatastore iotAccountDatastore;


       /*--------------------------------------------
    |         C O N S T R U C T O R S           |
    ============================================*/

    /**
     * Default no-argument constructor.
     */
    public IOTAccount() {
    }

    /**
     * Constructs a SimpleAccount instance for the specified realm with the given principal and credentials, with the
     * the assigned roles and permissions.
     *
     * @param principal   the 'primary' identifying attributes of the account, for example, a user id or username.
     * @param credentials the credentials that verify identity for the account
     * @param realmName   the name of the realm that accesses this account data
     */
    public IOTAccount(Object principal, Object credentials, String realmName) {
        this.principals = new SimplePrincipalCollection(principal, realmName);
        this.credentials = credentials;
    }

    public IOTAccountDatastore getIotAccountDatastore() {
        return iotAccountDatastore;
    }

    public void setIotAccountDatastore(IOTAccountDatastore iotAccountDatastore) {
        this.iotAccountDatastore = iotAccountDatastore;
    }

    /**
     * Returns <code>true</code> if this Account is locked and thus cannot be used to login, <code>false</code> otherwise.
     *
     * @return <code>true</code> if this Account is locked and thus cannot be used to login, <code>false</code> otherwise.
     */
    public boolean isLocked() {
        return locked;
    }

    /**
     * Sets whether or not the account is locked and can be used to login.
     *
     * @param locked <code>true</code> if this Account is locked and thus cannot be used to login, <code>false</code> otherwise.
     */
    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * Returns whether or not the Account's credentials are expired.  This usually indicates that the Subject or an application
     * administrator would need to change the credentials before the account could be used.
     *
     * @return whether or not the Account's credentials are expired.
     */
    public boolean isCredentialsExpired() {
        return credentialsExpired;
    }

    /**
     * Sets whether or not the Account's credentials are expired.  A <code>true</code> value indicates that the Subject
     * or application administrator would need to change their credentials before the account could be used.
     *
     * @param credentialsExpired <code>true</code> if this Account's credentials are expired and need to be changed,
     *                           <code>false</code> otherwise.
     */
    public void setCredentialsExpired(boolean credentialsExpired) {
        this.credentialsExpired = credentialsExpired;
    }


    /**
     * Returns the salt used to salt the account's credentials or {@code null} if no salt was used.
     *
     * @return the salt used to salt the account's credentials or {@code null} if no salt was used.
     */
    @Override
    public ByteSource getCredentialsSalt() {
        return this.credentialsSalt;
    }


    public void setCredentialsSalt(ByteSource credentialsSalt) {
        this.credentialsSalt = credentialsSalt;
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
        return this.principals;
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
        return this.credentials;
    }

    /**
     * Sets this Account's credentials that verify one or more of the Account's
     * {@link #getPrincipals() principals}, such as a password or private key.
     *
     * @param credentials the credentials associated with this Account that verify one or more of the Account principals.
     * @see org.apache.shiro.authc.Account#getCredentials()
     */
    public void setCredentials(Object credentials) {
        this.credentials = credentials;
    }


    /**
     * Returns the names of all roles assigned to a corresponding Subject.
     *
     * @return the names of all roles assigned to a corresponding Subject.
     */
    @Override
    public Collection<String> getRoles() {
        return roles;
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
             iotRole.getPermissions().forEach(permission -> {stringPermissions.add(permission.toString());});

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
     * Sets the Account's assigned roles.  Simply calls <code>this.authzInfo.setRoles(roles)</code>.
     *
     * @param roles the Account's assigned roles.
     * @see Account#getRoles()
     */
    public void setRoles(Set<String> roles) {
        this.roles = roles;
    }

    /**
     * Adds a role to this Account's set of assigned roles.  Simply delegates to
     * <code>this.authzInfo.addRole(role)</code>.
     *
     * @param role a role to assign to this Account.
     */
    public void addRole(String role) {
        if (this.roles == null) {
            this.roles = new HashSet<>();
        }
        this.roles.add(role);
    }

    /**
     * Adds one or more roles to this Account's set of assigned roles. Simply delegates to
     * <code>this.authzInfo.addRoles(roles)</code>.
     *
     * @param roles one or more roles to assign to this Account.
     */
    public void addRole(Collection<String> roles) {
        if (this.roles == null) {
            this.roles = new HashSet<>();
        }
        this.roles.addAll(roles);
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

    @Override
    public Serializable generateIdKey() throws UnRetriableException {

        IdConstruct idConstruct = (IdConstruct) getPrincipals().getPrimaryPrincipal();

        if(null == idConstruct ){
            throw new UnRetriableException(" Can't save an account without an id construct");
        }

        return createCacheKey(idConstruct.getPartition(), idConstruct.getUsername());

    }


    public static String createCacheKey(String partition, String username){
        return "p["+partition+ "]-"+ username;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {

            objectOutput.writeObject(getPrincipals());
            objectOutput.writeObject(getCredentials());
            objectOutput.writeObject(getCredentialsSalt());
            objectOutput.writeBoolean(isLocked());
            objectOutput.writeBoolean(isCredentialsExpired());
            objectOutput.writeObject(getRoles());
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        principals = (PrincipalCollection) objectInput.readObject();
        setCredentials(objectInput.readObject());
        setCredentialsSalt((ByteSource) objectInput.readObject());
        setLocked(objectInput.readBoolean());
        setCredentialsExpired(objectInput.readBoolean());
        setRoles((Set<String>) objectInput.readObject());
    }
}
