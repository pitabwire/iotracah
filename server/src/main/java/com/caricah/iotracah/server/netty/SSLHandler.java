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

package com.caricah.iotracah.server.netty;

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.security.KeyStore;


/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/27/15
 */
public class SSLHandler {

    private static final Logger log = LoggerFactory.getLogger(SSLHandler.class);

    public static final String CONFIGURATION_SERVER_SSL_KEYSTORE_TYPE = "system.internal.server.ssl.keystore.type";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYSTORE_TYPE = "PKCS12";

    public static final String CONFIGURATION_SERVER_SSL_TRUSTSTORE_TYPE = "system.internal.server.ssl.truststore.type";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_TRUSTSTORE_TYPE = "PKCS12";

    public static final String CONFIGURATION_SERVER_SSL_KEYMANAGER_PASSPHRASE = "system.internal.server.ssl.keymanager.passphrase";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYMANAGER_PASSPHRASE = "s3cr3t";

    public static final String CONFIGURATION_SERVER_SSL_KEYSTORE_PASSPHRASE = "system.internal.server.ssl.keystore.passphrase";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYSTORE_PASSPHRASE = "s3cr3t";

    public static final String CONFIGURATION_SERVER_USE_CUSTOM_SSL_TRUSTSTORE = "system.internal.server.use.custom.ssl.truststore";
    public static final boolean CONFIGURATION_VALUE_DEFAULT_SERVER_USE_CUSTOM_SSL_TRUSTSTORE = false;

    public static final String CONFIGURATION_SERVER_SSL_TRUSTSTORE_PASSPHRASE = "system.internal.server.ssl.truststore.passphrase";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_TRUSTSTORE_PASSPHRASE = "s3cr3t";

    public static final String CONFIGURATION_SERVER_SSL_KEYSTORE_FILE = "system.internal.server.ssl.keystore.file";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYSTORE_FILE = "keystore.pkcs12";

    public static final String CONFIGURATION_SERVER_SSL_TRUSTSTORE_FILE = "system.internal.server.ssl.truststore.file";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_TRUSTSTORE_FILE = "truststore.pkcs12";

    public static final String CONFIGURATION_SERVER_SSL_PROTOCAL = "system.internal.server.ssl.protocal";
    public static final String CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_PROTOCAL = "TLS";


    private final Configuration configuration;

    public SSLHandler(Configuration configuration){
        this.configuration = configuration;
    }


    public Configuration getConfiguration() {
        return configuration;
    }

    public SSLEngine getSSLEngine() throws UnRetriableException{

        try {

            String keystoreType = getConfiguration().getString(CONFIGURATION_SERVER_SSL_KEYSTORE_TYPE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYSTORE_TYPE);
            String truststoreType = getConfiguration().getString(CONFIGURATION_SERVER_SSL_TRUSTSTORE_TYPE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_TRUSTSTORE_TYPE);

            String keymanagerPassPhraseString =getConfiguration().getString(CONFIGURATION_SERVER_SSL_KEYMANAGER_PASSPHRASE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYMANAGER_PASSPHRASE);

            boolean useCustomTrustStore =getConfiguration().getBoolean(CONFIGURATION_SERVER_USE_CUSTOM_SSL_TRUSTSTORE, CONFIGURATION_VALUE_DEFAULT_SERVER_USE_CUSTOM_SSL_TRUSTSTORE);

            String keystorePassPhraseString =getConfiguration().getString(CONFIGURATION_SERVER_SSL_KEYSTORE_PASSPHRASE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYSTORE_PASSPHRASE);
            String truststorePassPhraseString =getConfiguration().getString(CONFIGURATION_SERVER_SSL_TRUSTSTORE_PASSPHRASE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_TRUSTSTORE_PASSPHRASE);

            String keystoreFile =getConfiguration().getString(CONFIGURATION_SERVER_SSL_KEYSTORE_FILE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_KEYSTORE_FILE);
            String truststoreFile =getConfiguration().getString(CONFIGURATION_SERVER_SSL_TRUSTSTORE_FILE, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_TRUSTSTORE_FILE);

            String protocal =getConfiguration().getString(CONFIGURATION_SERVER_SSL_PROTOCAL, CONFIGURATION_VALUE_DEFAULT_SERVER_SSL_PROTOCAL);

            KeyStore ks = KeyStore.getInstance(keystoreType);
            KeyStore ts = KeyStore.getInstance(truststoreType);

            char[] keymanagerPassPhrase = keymanagerPassPhraseString.toCharArray();
            char[] keystorePassPhrase = keystorePassPhraseString.toCharArray();
            char[] truststorePassPhrase = truststorePassPhraseString.toCharArray();

            ks.load(new FileInputStream(keystoreFile), keystorePassPhrase);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, keymanagerPassPhrase);

            TrustManager[] trustManagers = null;
            if( useCustomTrustStore ) {
                ts.load(new FileInputStream(truststoreFile), truststorePassPhrase);

                TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
                tmf.init(ts);

                trustManagers = tmf.getTrustManagers();
            }
            SSLContext sslContext = SSLContext.getInstance(protocal);

            sslContext.init(kmf.getKeyManagers(), trustManagers , null);

            return sslContext.createSSLEngine();

        }catch (Exception e){
            log.error(" getSSLEngine : problems when trying to initiate secure protocals", e);
            throw new UnRetriableException(e);
        }
    }
}
