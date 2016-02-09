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

package com.caricah.iotracah.bootstrap.security.realm.auth;

import org.apache.shiro.authc.AuthenticationToken;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IdPassToken implements AuthenticationToken {

    /*--------------------------------------------
    |             C O N S T A N T S             |
    ============================================*/

    /*--------------------------------------------
    |    I N S T A N C E   V A R I A B L E S    |
    ============================================*/

    /**
     * The identification construct
     */
    private IdConstruct idConstruct;

    /**
     * The password, in char[] format
     */
    private char[] password;


    public IdPassToken(String partition, String username, String clientId, char[] password){
        this.idConstruct = new IdConstruct(partition, username, clientId);
        this.password = password;
    }

    public IdConstruct getIdConstruct() {
        return idConstruct;
    }

    public void setIdConstruct(IdConstruct idConstruct) {
        this.idConstruct = idConstruct;
    }

    public char[] getPassword() {
        return password;
    }

    public void setPassword(char[] password) {
        this.password = password;
    }

    /**
     * Returns the account identity submitted during the authentication process.
     * <p>
     * <p>Most application authentications are username/password based and have this
     * object represent a username.  If this is the case for your application,
     * take a look at the {@link org.apache.shiro.authc.UsernamePasswordToken UsernamePasswordToken}, as it is probably
     * sufficient for your use.
     * <p>
     * <p>Ultimately, the object returned is application specific and can represent
     * any account identity (user id, X.509 certificate, etc).
     *
     * @return the account identity submitted during the authentication process.
     * @see org.apache.shiro.authc.UsernamePasswordToken
     */
    @Override
    public Object getPrincipal() {
        return getIdConstruct();
    }

    /**
     * Returns the credentials submitted by the user during the authentication process that verifies
     * the submitted {@link #getPrincipal() account identity}.
     * <p>
     * <p>Most application authentications are username/password based and have this object
     * represent a submitted password.  If this is the case for your application,
     * take a look at the {@link org.apache.shiro.authc.UsernamePasswordToken UsernamePasswordToken}, as it is probably
     * sufficient for your use.
     * <p>
     * <p>Ultimately, the credentials Object returned is application specific and can represent
     * any credential mechanism.
     *
     * @return the credential submitted by the user during the authentication process.
     */
    @Override
    public Object getCredentials() {
        return getPassword();
    }
}
