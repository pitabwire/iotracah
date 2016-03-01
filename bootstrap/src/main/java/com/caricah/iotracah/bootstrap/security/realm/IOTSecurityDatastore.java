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

import com.caricah.iotracah.bootstrap.security.realm.state.IOTAccount;
import com.caricah.iotracah.bootstrap.security.realm.state.IOTRole;
import org.apache.shiro.session.mgt.eis.SessionDAO;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public interface IOTSecurityDatastore extends SessionDAO {


    IOTAccount getIOTAccount(String partition, String username);

    void saveIOTAccount(IOTAccount account);



    IOTRole getIOTRole(String partition, String rolename);


    void saveIOTRole(IOTRole iotRole);

}
