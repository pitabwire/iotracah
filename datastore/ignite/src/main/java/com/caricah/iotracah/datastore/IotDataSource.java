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

package com.caricah.iotracah.datastore;

import javax.naming.*;


/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 2/26/16
 */
public class IotDataSource {


    private static IotDataSource datasource;

    private IotDataSource(){

    }

    public void setupDatasource(String driver, String dbUrl, String username, String password) throws NamingException {

        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.fscontext.RefFSContextFactory");
        System.setProperty(Context.PROVIDER_URL, "file:////tmp");

        Context ctx = new InitialContext();

        // Construct DriverAdapterCPDS reference
        Reference cpdsRef = new Reference("org.apache.commons.dbcp2.cpdsadapter.DriverAdapterCPDS",
                "org.apache.commons.dbcp2.cpdsadapter.DriverAdapterCPDS", null);
        cpdsRef.add(new StringRefAddr("driver", driver));
        cpdsRef.add(new StringRefAddr("url", dbUrl));
        cpdsRef.add(new StringRefAddr("user", username));
        cpdsRef.add(new StringRefAddr("password", password));
        ctx.rebind("jdbc_cpds", cpdsRef);

        Reference ref = new Reference("org.apache.commons.dbcp2.datasources.SharedPoolDataSource","org.apache.commons.dbcp2.datasources.SharedPoolDataSourceFactory", null);
        ref.add(new StringRefAddr("dataSourceName", "jdbc_cpds"));
        ref.add(new StringRefAddr("defaultMaxTotal", "100"));
        ref.add(new StringRefAddr("defaultMaxIdle", "30"));
        ref.add(new StringRefAddr("defaultMaxWaitMillis", "10000"));

        ctx.rebind("jdbc_commonpool",ref);

    }

    public static IotDataSource getInstance(){
        if (datasource == null) {
            datasource = new IotDataSource();
            return datasource;
        } else {
            return datasource;
        }
    }

}
