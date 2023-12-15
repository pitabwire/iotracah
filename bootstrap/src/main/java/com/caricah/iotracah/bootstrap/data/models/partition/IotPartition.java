/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.caricah.iotracah.bootstrap.data.models.partition;

import java.io.*;

/**
 * IotPartition definition.
 *
 * Code generated by Apache Ignite Schema Import utility: 02/23/2016.
 */
public class IotPartition implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Value for dateCreated. */
    private java.sql.Timestamp dateCreated;

    /** Value for dateModified. */
    private java.sql.Timestamp dateModified;

    /** Value for isActive. */
    private boolean isActive;

    /** Value for name. */
    private String name;

    /** Value for description. */
    private String description;

    /** Value for locked. */
    private boolean locked;

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
     * Gets description.
     *
     * @return Value for description.
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets description.
     *
     * @param description New value for description.
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Gets locked.
     *
     * @return Value for locked.
     */
    public boolean getLocked() {
        return locked;
    }

    /**
     * Sets locked.
     *
     * @param locked New value for locked.
     */
    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof IotPartition))
            return false;

        IotPartition that = (IotPartition)o;

        if (dateCreated != null ? !dateCreated.equals(that.dateCreated) : that.dateCreated != null)
            return false;

        if (dateModified != null ? !dateModified.equals(that.dateModified) : that.dateModified != null)
            return false;

        if (isActive != that.isActive)
            return false;

        if (name != null ? !name.equals(that.name) : that.name != null)
            return false;

        if (description != null ? !description.equals(that.description) : that.description != null)
            return false;

        if (locked != that.locked)
            return false;

        return true;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dateCreated != null ? dateCreated.hashCode() : 0;

        res = 31 * res + (dateModified != null ? dateModified.hashCode() : 0);

        res = 31 * res + (isActive ? 1 : 0);

        res = 31 * res + (name != null ? name.hashCode() : 0);

        res = 31 * res + (description != null ? description.hashCode() : 0);

        res = 31 * res + (locked ? 1 : 0);

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IotPartition [dateCreated=" + dateCreated +
            ", dateModified=" + dateModified +
            ", isActive=" + isActive +
            ", name=" + name +
            ", description=" + description +
            ", locked=" + locked +
            "]";
    }
}
