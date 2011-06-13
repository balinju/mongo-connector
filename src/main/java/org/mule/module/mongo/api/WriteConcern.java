/**
 * Mule MongoDB Cloud Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.module.mongo.api;

import com.mongodb.DB;

/**
 * An enum wrapper of {@link com.mongodb.WriteConcern}
 */
public enum WriteConcern
{
    /** No exceptions are raised, even for network issues */
    NONE(com.mongodb.WriteConcern.NONE),
    /** Exceptions are raised for network issues, but not server errors */
    NORMAL(com.mongodb.WriteConcern.NORMAL),
    /**
     * Exceptions are raised for network issues, and server errors, waits on a server
     * for the write operation
     */
    SAFE(com.mongodb.WriteConcern.SAFE),
    /**
     * Exceptions are raised for network issues, and server errors and the write
     * operation waits for the server to flush the data to disk
     */
    FSYNC_SAFE(com.mongodb.WriteConcern.FSYNC_SAFE),
    /**
     * Exceptions are raised for network issues, and server errors, waits for at
     * least 2 servers for the write operation
     */
    REPLICAS_SAFE(com.mongodb.WriteConcern.REPLICAS_SAFE),

    /** Database default write access */
    DATABASE_DEFAULT(null)
    {
        @Override
        public com.mongodb.WriteConcern getMongoWriteConcern(DB db)
        {
            return db.getWriteConcern();
        }
    };

    private final com.mongodb.WriteConcern mongoWriteConcern;

    private WriteConcern(com.mongodb.WriteConcern mongoWriteConcern)
    {
        this.mongoWriteConcern = mongoWriteConcern;
    }

    public com.mongodb.WriteConcern getMongoWriteConcern(DB db)
    {
        return mongoWriteConcern;
    }

}
