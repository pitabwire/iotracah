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

package com.caricah.iotracah.security.realm.state;

import com.caricah.iotracah.data.IdKeyComposer;
import com.caricah.iotracah.exceptions.UnRetriableException;
import org.apache.shiro.authz.Permission;

import java.io.*;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IOTRole implements IdKeyComposer, Externalizable {

    protected String partition = "";
    protected String name = null;
    protected Set<Permission> permissions;

    public IOTRole() {
    }

    public IOTRole(String partition, String name) {
        setPartition(partition);
        setName(name);
    }

    public IOTRole(String partition, String name, Set<Permission> permissions) {
        setPartition(partition);
        setName(name);
        setPermissions(permissions);
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<Permission> getPermissions() {
        return permissions;
    }

    public void setPermissions(Set<Permission> permissions) {
        this.permissions = permissions;
    }

    public void add(Permission permission) {
        Set<Permission> permissions = getPermissions();
        if (permissions == null) {
            permissions = new LinkedHashSet<>();
            setPermissions(permissions);
        }
        permissions.add(permission);
    }

    public void addAll(Collection<Permission> perms) {
        if (perms != null && !perms.isEmpty()) {
            Set<Permission> permissions = getPermissions();
            if (permissions == null) {
                permissions = new LinkedHashSet<>(perms.size());
                setPermissions(permissions);
            }
            permissions.addAll(perms);
        }
    }

    public boolean isPermitted(Permission p) {
        Collection<Permission> perms = getPermissions();
        if (perms != null && !perms.isEmpty()) {
            for (Permission perm : perms) {
                if (perm.implies(p)) {
                    return true;
                }
            }
        }
        return false;
    }

    public int hashCode() {
        return (getName() != null ? getName().hashCode() : 0);
    }

    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof IOTRole) {
            IOTRole ir = (IOTRole) o;
            //only check name, since role names should be unique across an entire application:
            return (getName() != null ? getName().equals(ir.getName()) : ir.getName() == null);
        }
        return false;
    }

    @Override
    public Serializable generateIdKey() throws UnRetriableException {

        if(null == getName() ){
            throw new UnRetriableException(" Can't save a role without a name");
        }

        return createCacheKey(getPartition(), getName());

    }

    public static String createCacheKey(String partition, String rolename){
        return partition + "-" + rolename;
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        objectOutput.writeObject(getPartition());
        objectOutput.writeObject(getName());
        objectOutput.writeObject(getPermissions());

    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {

        setPartition((String) objectInput.readObject());
        setName((String) objectInput.readObject());
        setPermissions((Set<Permission>)objectInput.readObject());
    }
}
