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

package com.caricah.iotracah.security.realm;

import com.caricah.iotracah.security.realm.auth.IdConstruct;
import com.caricah.iotracah.security.realm.auth.IdPassToken;
import com.caricah.iotracah.security.realm.state.IOTAccount;
import com.caricah.iotracah.security.realm.state.IOTRole;
import org.apache.shiro.authc.*;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.AuthorizingRealm;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.util.CollectionUtils;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public abstract class IOTAbstractRealm extends AuthorizingRealm{

    private IOTAccountDatastore iotAccountDatastore;


    public IOTAbstractRealm(){

        //IOTAbstractRealm is in memory data grid reloaded
        // - no need for an additional cache mechanism since we're
        //already as memory-efficient as one can be:
        setCachingEnabled(false);

    }


    public IOTAccountDatastore getIotAccountDatastore() {
        return iotAccountDatastore;
    }

    public void setIotAccountDatastore(IOTAccountDatastore iotAccountDatastore) {
        this.iotAccountDatastore = iotAccountDatastore;
    }

    /**
     * Retrieves the AuthorizationInfo for the given principals from the underlying data store.  When returning
     * an instance from this method, you might want to consider using an instance of
     * {@link SimpleAuthorizationInfo SimpleAuthorizationInfo}, as it is suitable in most cases.
     *
     * @param principals the primary identifying principals of the AuthorizationInfo that should be retrieved.
     * @return the AuthorizationInfo associated with this principals.
     * @see SimpleAuthorizationInfo
     */
    @Override
    protected AuthorizationInfo doGetAuthorizationInfo(PrincipalCollection principals) {

        IdConstruct idConstruct = (IdConstruct) getAvailablePrincipal(principals);

        return getIOTAccount(idConstruct.getPartition(), idConstruct.getUsername());
    }



    /**
     * Retrieves authentication data from an implementation-specific datasource (RDBMS, LDAP, etc) for the given
     * authentication token.
     * <p>
     * For most datasources, this means just 'pulling' authentication data for an associated subject/user and nothing
     * more and letting Shiro do the rest.  But in some systems, this method could actually perform EIS specific
     * log-in logic in addition to just retrieving data - it is up to the Realm implementation.
     * <p>
     * A {@code null} return value means that no account could be associated with the specified token.
     *
     * @param token the authentication token containing the user's principal and credentials.
     * @return an {@link AuthenticationInfo} object containing account data resulting from the
     * authentication ONLY if the lookup is successful (i.e. account exists and is valid, etc.)
     * @throws AuthenticationException if there is an error acquiring data or performing
     *                                 realm-specific authentication logic for the specified <tt>token</tt>
     */
    @Override
    protected AuthenticationInfo doGetAuthenticationInfo(AuthenticationToken token) throws AuthenticationException {

        IdConstruct idConstruct = ((IdPassToken) token).getIdConstruct();
        IOTAccount account = getIOTAccount(idConstruct.getPartition(), idConstruct.getUsername());

        if (account != null) {

            if (account.isLocked()) {
                throw new LockedAccountException("Account [" + account + "] is locked.");
            }
            if (account.isCredentialsExpired()) {
                String msg = "The credentials for account [" + account + "] are expired";
                throw new ExpiredCredentialsException(msg);
            }

        }

        return account;

    }

    public IOTAccount getIOTAccount(String partition, String username){
        IOTAccount account= getIotAccountDatastore().getIOTAccount(partition, username);

        if(null != account)
            account.setIotAccountDatastore(getIotAccountDatastore());

        return account;
    }

    public IOTAccount addIOTAccount(String partition, String username, String password) {

        IdConstruct idConstruct = new IdConstruct(partition, username, null);
        IOTAccount account = new IOTAccount(idConstruct, password, getName());
        saveIOTAccount(account);
        return getIOTAccount(partition, username);
    }

    protected void saveIOTAccount(IOTAccount iotAccount){
        getIotAccountDatastore().saveIOTAccount(iotAccount);
    }

    protected IOTRole getIOTRole(String partition, String rolename) {
        return getIotAccountDatastore().getIOTRole(partition, rolename);
    }

    public boolean roleExists(String partition, String name) {
        return getIotAccountDatastore().getIOTRole(partition, name) != null;
    }

    public IOTRole addIOTRole(String partition, String rolename ) {
        saveIOTRole( new IOTRole(partition, rolename));
        return getIotAccountDatastore().getIOTRole(partition, rolename);

    }

    public void saveIOTRole(IOTRole iotRole) {
        getIotAccountDatastore().saveIOTRole(iotRole);

    }


    @Override
    public boolean supports(AuthenticationToken token) {
        return token != null && IdPassToken.class.isAssignableFrom(token.getClass());
    }
}
