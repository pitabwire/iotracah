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

package com.caricah.iotracah.bootstrap.system;

import com.caricah.iotracah.bootstrap.exceptions.UnRetriableException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class ResourceFileUtil {


    public static File getFileFromResource(Class classInPackage, String resource) throws UnRetriableException {

        try {

            File file = new File(resource);

            if(file.exists()){
                return file;
            }

            URL res = classInPackage.getResource("/"+resource);

            if(null == res){
                throw new UnRetriableException("The file ["+resource+"] was not located within the system.");
            }


            if (res.toString().startsWith("jar:")) {
                InputStream input = classInPackage.getResourceAsStream("/"+resource);

                Files.copy(input, file.toPath());
            }else {
               Files.copy(Paths.get(res.getFile()), file.toPath(), StandardCopyOption.COPY_ATTRIBUTES );
            }
            return file;

        } catch (IOException ex) {
            throw new UnRetriableException(ex);
        }


    }
}
