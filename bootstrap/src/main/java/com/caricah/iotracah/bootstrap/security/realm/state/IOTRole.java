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

import com.caricah.iotracah.bootstrap.security.realm.auth.permission.IOTPermission;
import com.caricah.iotracah.bootstrap.security.realm.auth.permission.IOTPermissionResolver;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * @author <a href="mailto:bwire@caricah.com"> Peter Bwire </a>
 * @version 1.0 10/6/15
 */
public class IOTRole implements Serializable {

    private static final PermissionResolver pr = new IOTPermissionResolver();
    private transient Set<Permission> permissions;

    /** */
    private static final long serialVersionUID = 0L;

    /** Value for id. */
    private long id;

    /** Value for dateCreated. */
    private java.sql.Timestamp dateCreated;

    /** Value for dateModified. */
    private java.sql.Timestamp dateModified;

    /** Value for isActive. */
    private boolean isActive;

    /** Value for name. */
    private String name;

    /** Value for permissionList. */
    private String permissionList;

    /** Value for partitionId. */
    private String partitionId;


    public IOTRole() {
    }

    public IOTRole(String partition, String name) {
        setPartitionId(partition);
        setName(name);
    }

    /**
     * Gets id.
     *
     * @return Value for id.
     */
    public long getId() {
        return id;
    }

    /**
     * Sets id.
     *
     * @param id New value for id.
     */
    public void setId(long id) {
        this.id = id;
    }

    /**
     * Gets dateCreated.
     *
     * @return Value for dateCreated.
     */
    public java.sql.Timestamp getDateCreated() {
        return dateCreated;
    }

    /**
     * Sets dateCreated.
     *
     * @param dateCreated New value for dateCreated.
     */
    public void setDateCreated(java.sql.Timestamp dateCreated) {
        this.dateCreated = dateCreated;
    }

    /**
     * Gets dateModified.
     *
     * @return Value for dateModified.
     */
    public java.sql.Timestamp getDateModified() {
        return dateModified;
    }

    /**
     * Sets dateModified.
     *
     * @param dateModified New value for dateModified.
     */
    public void setDateModified(java.sql.Timestamp dateModified) {
        this.dateModified = dateModified;
    }

    /**
     * Gets isActive.
     *
     * @return Value for isActive.
     */
    public boolean getIsActive() {
        return isActive;
    }

    /**
     * Sets isActive.
     *
     * @param isActive New value for isActive.
     */
    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    /**
     * Gets name.
     *
     * @return Value for name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets name.
     *
     * @param name New value for name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets permissionList.
     *
     * @return Value for permissionList.
     */
    public String getPermissionList() {
        return permissionList;
    }

    /**
     * Sets permissionList.
     *
     * @param permissionList New value for permissionList.
     */
    public void setPermissionList(String permissionList) {
        this.permissionList = permissionList;
    }

    /**
     * Gets partitionId.
     *
     * @return Value for partitionId.
     */
    public String getPartitionId() {
        return partitionId;
    }

    /**
     * Sets partitionId.
     *
     * @param partitionId New value for partitionId.
     */
    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IOTRole))
            return false;

        IOTRole that = (IOTRole)o;

        if (id != that.id)
            return false;

        if (dateCreated != null ? !dateCreated.equals(that.dateCreated) : that.dateCreated != null)
            return false;

        if (dateModified != null ? !dateModified.equals(that.dateModified) : that.dateModified != null)
            return false;

        if (isActive != that.isActive)
            return false;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;

        if (permissionList != null ? !permissionList.equals(that.permissionList) : that.permissionList != null)
            return false;

        if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {


       int res = 31 * (int) id + (dateCreated != null ? dateCreated.hashCode() : 0);

        res = 31 * res + (dateModified != null ? dateModified.hashCode() : 0);

        res = 31 * res + (isActive ? 1 : 0);

        res = 31 * res + (name != null ? name.hashCode() : 0);

        res = 31 * res + (permissionList != null ? permissionList.hashCode() : 0);

        res = 31 * res + (partitionId != null ? partitionId.hashCode() : 0);

        return res;
    }


    public Set<Permission> getPermissions() {

        if(Objects.isNull(permissions) ){

            permissions = new HashSet<>();

        }

        if(permissions.isEmpty()){

            if(Objects.nonNull(getPermissionList())) {
              for (String wildCard: getPermissionList().split(",")){

                  permissions.add(pr.resolvePermission(wildCard));
              }
            }

        }

        return permissions;
    }



    public void add(IOTPermission permission) {

        String permissionString = permission.getWildcard();

        String permissionStringList;
        if(Objects.isNull(getPermissionList()) || getPermissionList().isEmpty()){
            permissionStringList = permissionString;
        }else{
            permissionStringList = getPermissionList()+","+permissionString;
        }

        setPermissionList(permissionStringList);

    }

  public void add(Set<IOTPermission> permissionList) {
      permissionList.forEach(this::add);
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

}
