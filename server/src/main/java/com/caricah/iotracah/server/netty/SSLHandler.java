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
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslProvider;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 5/27/15
 */
public class SSLHandler {

    private static final Logger log = LoggerFactory.getLogger(SSLHandler.class);

    public static final String CONFIG_SERVER_SSL_CERTIFICATE_KEY_PASSPHRASE = "config.server.ssl.certificate.key.passphrase";
    public static final String CONFIG_SERVER_SSL_CERTIFICATE_KEY_PASSPHRASE_DEFAULT_VALUE = null;

    public static final String CONFIG_SERVER_SSL_CERTIFICATE_KEY_FILE = "config.server.ssl.certificate.key.file";
    public static final String CONFIG_SERVER_SSL_CERTIFICATE_KEY_FILE_DEFAULT_VALUE = "keystore.pem";

    public static final String CONFIG_SERVER_SSL_CERTIFICATE_CHAIN_FILE = "config.server.ssl.certificate.cert.file";
    public static final String CONFIG_SERVER_SSL_CERTIFICATE_CHAIN_FILE_DEFAULT_VALUE = "truststore.pem";

    private final Configuration configuration;

    public SSLHandler(Configuration configuration){
        this.configuration = configuration;
    }


    public Configuration getConfiguration() {
        return configuration;
    }

    public SslContext getSslContext() throws UnRetriableException{


        try {

            File certificateChainFile = getCertificateChainFile();
            File certificateKeyFile = getCertificateKeyFile();
            String keyPassword = getKeyPassword();

            SslProvider sslProvider;
            if(OpenSsl.isAvailable()) {
                sslProvider  = SslProvider.OPENSSL;
            }else{
                sslProvider  = SslProvider.JDK;
            }

            return SslContext.newServerContext(sslProvider, certificateChainFile, certificateKeyFile, keyPassword  );

        }catch (Exception e){
            log.error(" getSSLEngine : problems when trying to initiate secure protocals", e);
            throw new UnRetriableException(e);
        }
    }

    private String getKeyPassword() {
        return getConfiguration().getString(CONFIG_SERVER_SSL_CERTIFICATE_KEY_PASSPHRASE, CONFIG_SERVER_SSL_CERTIFICATE_KEY_PASSPHRASE_DEFAULT_VALUE);
    }

    private File getCertificateKeyFile() {

        String keyFile =getConfiguration().getString(CONFIG_SERVER_SSL_CERTIFICATE_KEY_FILE, CONFIG_SERVER_SSL_CERTIFICATE_KEY_FILE_DEFAULT_VALUE);
        return new File(keyFile);
    }

    private File getCertificateChainFile() {
        String chainFile =getConfiguration().getString(CONFIG_SERVER_SSL_CERTIFICATE_CHAIN_FILE, CONFIG_SERVER_SSL_CERTIFICATE_CHAIN_FILE_DEFAULT_VALUE);

        return new File(chainFile);
    }
}
