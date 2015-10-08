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

package com.caricah.iotracah.security.realm.auth.permission;

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.InvalidPermissionStringException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A <code>WildcardPermission</code> is a very flexible permission construct supporting multiple levels of
 * permission matching. However, most people will probably follow some standard conventions as explained below.
 * <p>
 * <h3>Simple Usage</h3>
 * <p>
 * In the simplest form, <code>WildcardPermission</code> can be used as a simple permission string. You could grant a
 * user an &quot;editNewsletter&quot; permission and then check to see if the user has the editNewsletter
 * permission by calling
 * <p>
 * <code>subject.isPermitted(&quot;editNewsletter&quot;)</code>
 * <p>
 * This is (mostly) equivalent to
 * <p>
 * <code>subject.isPermitted( new WildcardPermission(&quot;editNewsletter&quot;) )</code>
 * <p>
 * but more on that later.
 * <p>
 * The simple permission string may work for simple applications, but it requires you to have security like
 * <code>&quot;viewNewsletter&quot;</code>, <code>&quot;deleteNewsletter&quot;</code>,
 * <code>&quot;createNewsletter&quot;</code>, etc. You can also grant a user <code>&quot;*&quot;</code> security
 * using the wildcard character (giving this class its name), which means they have <em>all</em> security. But
 * using this approach there's no way to just say a user has &quot;all newsletter security&quot;.
 * <p>
 * For this reason, <code>WildcardPermission</code> supports multiple <em>levels</em> of permissioning.
 * <p>
 * <h3>Multiple Levels</h3>
 * <p>
 * WildcardPermission</code> also supports the concept of multiple <em>levels</em>.  For example, you could
 * restructure the previous simple example by granting a user the permission <code>&quot;newsletter:edit&quot;</code>.
 * The colon in this example is a special character used by the <code>WildcardPermission</code> that delimits the
 * next token in the permission.
 * <p>
 * In this example, the first token is the <em>domain</em> that is being operated on
 * and the second token is the <em>action</em> being performed. Each level can contain multiple values.  So you
 * could simply grant a user the permission <code>&quot;newsletter:view,edit,create&quot;</code> which gives them
 * access to perform <code>view</code>, <code>edit</code>, and <code>create</code> actions in the <code>newsletter</code>
 * <em>domain</em>. Then you could check to see if the user has the <code>&quot;newsletter:create&quot;</code>
 * permission by calling
 * <p>
 * <code>subject.isPermitted(&quot;newsletter:create&quot;)</code>
 * <p>
 * (which would return true).
 * <p>
 * In addition to granting multiple security via a single string, you can grant all permission for a particular
 * level. So if you wanted to grant a user all actions in the <code>newsletter</code> domain, you could simply give
 * them <code>&quot;newsletter:*&quot;</code>. Now, any permission check for <code>&quot;newsletter:XXX&quot;</code>
 * will return <code>true</code>. It is also possible to use the wildcard token at the domain level (or both): so you
 * could grant a user the <code>&quot;view&quot;</code> action across all domains <code>&quot;*:view&quot;</code>.
 * <p>
 * <h3>Instance-level Access Control</h3>
 * <p>
 * Another common usage of the <code>WildcardPermission</code> is to model instance-level Access Control Lists.
 * In this scenario you use three tokens - the first is the <em>domain</em>, the second is the <em>action</em>, and
 * the third is the <em>instance</em> you are acting on.
 * <p>
 * So for example you could grant a user <code>&quot;newsletter:edit:12,13,18&quot;</code>.  In this example, assume
 * that the third token is the system's ID of the newsletter. That would allow the user to edit newsletters
 * <code>12</code>, <code>13</code>, and <code>18</code>. This is an extremely powerful way to express security,
 * since you can now say things like <code>&quot;newsletter:*:13&quot;</code> (grant a user all actions for newsletter
 * <code>13</code>), <code>&quot;newsletter:view,create,edit:*&quot;</code> (allow the user to
 * <code>view</code>, <code>create</code>, or <code>edit</code> <em>any</em> newsletter), or
 * <code>&quot;newsletter:*:*</code> (allow the user to perform <em>any</em> action on <em>any</em> newsletter).
 * <p>
 * To perform checks against these instance-level security, the application should include the instance ID in the
 * permission check like so:
 * <p>
 * <code>subject.isPermitted( &quot;newsletter:edit:13&quot; )</code>
 * <p>
 * There is no limit to the number of tokens that can be used, so it is up to your imagination in terms of ways that
 * this could be used in your application.  However, the Shiro team likes to standardize some common usages shown
 * above to help people get started and provide consistency in the Shiro community.
 *
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/4/15
 * @since 0.9
 */
public class IOTPermission implements Permission, Serializable {

    protected final Logger log = LoggerFactory.getLogger(IOTPermission.class);
    /*--------------------------------------------
     |             C O N S T A N T S             |
     ============================================*/
    protected static final String MULTI_LEVEL_WILDCARD_TOKEN = "#";
    protected static final String SINGLE_LEVEL_WILDCARD_TOKEN = "+";
    protected static final String PART_DIVIDER_TOKEN = "/";
    protected static final String USERNAME_TOKEN = "%u";
    protected static final String PARTITION_TOKEN = "%p";
    protected static final String CLIENT_ID_TOKEN = "%c";
    protected static final boolean DEFAULT_CASE_SENSITIVE = false;

    /*--------------------------------------------
    |    I N S T A N C E   V A R I A B L E S    |
    ============================================*/
    private List<String> parts;

    private String type;

    private String username;

    private String partition;

    private String clientId;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public IOTPermission(String wildcardString) {
        this(wildcardString, DEFAULT_CASE_SENSITIVE);


    }

    public IOTPermission(String wildcardString, boolean caseSensitive) {
        setParts(wildcardString, caseSensitive);

        log.info("*********************************************************************");
        log.info("*********      Creating a an IOTPermission from : {}    *************", wildcardString);
        log.info("*********************************************************************");
    }

  public IOTPermission(String partition, String username, String clientId, String wildcardString) {
        this(wildcardString);
      setUsername(username);
      setPartition(partition);
      setClientId(clientId);
    }


    protected void setParts(String wildcardString, boolean caseSensitive) {
        if (wildcardString == null || wildcardString.trim().isEmpty()) {
            throw new InvalidPermissionStringException("string cannot be null or empty.", wildcardString);
        }


        wildcardString = wildcardString.trim();

        int indexOfColon = wildcardString.indexOf(":");

        if (indexOfColon < 0) {
            throw new IllegalArgumentException("Client permission must be prefixed with the permission type.");
        }

        setType(wildcardString.substring(0, indexOfColon));

        wildcardString = wildcardString.substring(indexOfColon + 1);

        if ( wildcardString.trim().isEmpty()) {
            throw new InvalidPermissionStringException("string cannot be null or empty.", wildcardString);
        }

        if (!caseSensitive) {
            wildcardString = wildcardString.toLowerCase();
        }


        this.parts = Arrays.asList(wildcardString.split(Pattern.quote(PART_DIVIDER_TOKEN)));

        boolean isFirst = true;

        for (String part : getParts()) {

            if (part.isEmpty() ) {

                if(isFirst){
                    isFirst = false;
                }else

                throw new InvalidPermissionStringException("permission string cannot" +
                        " contain parts with only dividers. Make sure permission strings" +
                        " are properly formatted ", wildcardString);
            }

         }

        if (this.getParts().isEmpty()) {
            throw new IllegalArgumentException("Client permission string cannot contain only dividers. Make sure permission strings are properly formatted.");
        }
    }


    /*--------------------------------------------
    |  A C C E S S O R S / M O D I F I E R S    |
    ============================================*/
    protected List<String> getParts() {
        return this.parts;
    }


    /**
     * Returns {@code true} if this current instance <em>implies</em> all the functionality and/or resource access
     * described by the specified {@code Permission} argument, {@code false} otherwise.
     * <p>
     * <p>That is, this current instance must be exactly equal to or a <em>superset</em> of the functionalty
     * and/or resource access described by the given {@code Permission} argument.  Yet another way of saying this
     * would be:
     * <p>
     * <p>If &quot;permission1 implies permission2&quot;, i.e. <code>permission1.implies(permission2)</code> ,
     * then any Subject granted {@code permission1} would have ability greater than or equal to that defined by
     * {@code permission2}.
     *
     * @param p the permission to check for behavior/functionality comparison.
     * @return {@code true} if this current instance <em>implies</em> all the functionality and/or resource access
     * described by the specified {@code Permission} argument, {@code false} otherwise.
     */
    @Override
    public boolean implies(Permission p) {

        // By default only supports comparisons with other WildcardPermissions
        if (!(p instanceof IOTPermission)) {
            return false;
        }

        IOTPermission otherP = (IOTPermission) p;

        if(!getType().equals(otherP.getType())){
           return false;
        }


        List<String> otherParts = otherP.getParts();

        int i = 0;
        for (String otherPart : otherParts) {
            // If this permission has less parts than the other permission,
            // everything after the number of parts contained
            // in this permission is automatically implied, so return true
            if (getParts().size() - 1 < i) {
                return true;
            } else {
                String part = getParts().get(i);

                if (!Objects.equals(part, otherPart)) {

                    //Check username
                    switch (part) {
                        case USERNAME_TOKEN:
                            if (!Objects.equals(otherP.getUsername(), otherPart))
                                return false;
                            break;
                        case PARTITION_TOKEN:
                            if (!Objects.equals(otherP.getPartition(), otherPart))
                                return false;
                            break;
                        case CLIENT_ID_TOKEN:

                            if (!Objects.equals(otherP.getClientId(), otherPart))
                                return false;
                            break;
                        default:
                            if (!( Objects.equals(MULTI_LEVEL_WILDCARD_TOKEN, part)
                                    || Objects.equals(SINGLE_LEVEL_WILDCARD_TOKEN, part)))
                                return false;
                    }
                }
            }
            i++;
        }


        // If this permission has more parts than the other parts,
        // only imply it if all of the other parts are wildcards
        for (; i < getParts().size(); i++) {
            String part = getParts().get(i);
            if (!MULTI_LEVEL_WILDCARD_TOKEN.equals(part)) {
                return false;
            }
        }

        log.info("*********************************************************************");
        log.info("*********      Successfully implied {} -> {}    *************", this, otherP);
        log.info("*********************************************************************");


        return true;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();

           for (String part : parts) {
            if(buffer.length()==0){
                buffer.append(getType()).append(":");
            }else {
                buffer.append("/");
            }
                buffer.append(part);
        }
        return buffer.toString();
    }

    public boolean equals(Object o) {
        if (o instanceof IOTPermission) {
            IOTPermission clp = (IOTPermission) o;
            return parts.equals(clp.parts);
        }
        return false;
    }

    public int hashCode() {
        return parts.hashCode();
    }

}
