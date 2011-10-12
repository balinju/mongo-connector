/**
 * Mule MongoDB Object Store
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.module.mongo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Module;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.PartitionableExpirableObjectStore;
import org.mule.module.mongo.api.MongoClient;
import org.mule.module.mongo.api.MongoClientAdaptor;
import org.mule.module.mongo.api.WriteConcern;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * A PartitionableExpirableObjectStore backed by MongoDB.
 * 
 * @author MuleSoft Inc.
 */
@Module(name = "mongo-object-store", schemaVersion = "2.0")
public class MongoObjectStore implements PartitionableExpirableObjectStore<Serializable>
{
    private static final String OBJECTSTORE_COLLECTION_PREFIX = "mule.objectstore.";
    private static final String OBJECTSTORE_DEFAULT_PARTITION_NAME = "_default";

    /**
     * The host of the Mongo server
     */
    @Configurable
    @Optional
    @Default("localhost")
    private String host;
    /**
     * The port of the Mongo server
     */
    @Configurable
    @Optional
    @Default("27017")
    private int port;
    /**
     * The database name of the Mongo server
     */
    @Configurable
    @Optional
    @Default("test")
    private String database;
    /**
     * The default concern to use to when writing to Mongo
     */
    @Configurable
    @Optional
    @Default("DATABASE_DEFAULT")
    private WriteConcern defaultWriteConcern;

    // FIXME add username and password

    public boolean isPersistent()
    {
        return true;
    }

    public void open() throws ObjectStoreException
    {
        open(OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public void close() throws ObjectStoreException
    {
        close(OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public List<Serializable> allKeys() throws ObjectStoreException
    {
        return allKeys(OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public void expire(int entryTTL, int maxEntries) throws ObjectStoreException
    {
        expire(entryTTL, maxEntries, OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public boolean contains(Serializable key) throws ObjectStoreException
    {
        return contains(key, OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public void store(Serializable key, Serializable value) throws ObjectStoreException
    {
        store(key, value, OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public Serializable retrieve(Serializable key) throws ObjectStoreException
    {
        return retrieve(key, OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public Serializable remove(Serializable key) throws ObjectStoreException
    {
        return remove(key, OBJECTSTORE_DEFAULT_PARTITION_NAME);
    }

    public void open(String partitionName) throws ObjectStoreException
    {
        // NOOP
    }

    public void close(String partitionName) throws ObjectStoreException
    {
        // NOOP
    }

    public boolean contains(Serializable key, String partitionName) throws ObjectStoreException
    {
        // FIXME implement!
        return false;
    }

    public void store(Serializable key, Serializable value, String partitionName) throws ObjectStoreException
    {
        // MongoSession session = null;
        // try
        // {
        // // FIXME support username and password
        // session = createSession(null, null);
        //
        // String collection = OBJECTSTORE_COLLECTION_PREFIX + partitionName;
        // if (!existsCollection(session, collection))
        // {
        // createCollection(session, collection, false, null, null);
        // createIndex(session, collection, "timestamp", IndexOrder.ASC);
        // }
        //
        // byte[] keyAsBytes = SerializationUtils.serialize(key);
        // String keyDigest = DigestUtils.md5DigestAsHex(keyAsBytes);
        // // FIXME must throw ObjectAlreadyExistException if already present
        // DBObject dbObject = new BasicDBObject();
        // dbObject.put("_id", keyDigest);
        // dbObject.put("timestamp", System.currentTimeMillis());
        // dbObject.put("key", keyAsBytes);
        // dbObject.put("value", SerializationUtils.serialize(value));
        // insertObject(session, collection, dbObject, getDefaultWriteConcern());
        // }
        // catch (UnknownHostException uoe)
        // {
        // throw new ObjectStoreNotAvaliableException(uoe);
        // }
        // finally
        // {
        // if (session != null) destroySession(session);
        // }
    }

    public Serializable retrieve(Serializable key, String partitionName) throws ObjectStoreException
    {
        // FIXME implement!
        throw new ObjectDoesNotExistException();
    }

    public Serializable remove(Serializable key, String partitionName) throws ObjectStoreException
    {
        // FIXME implement!
        return null;
    }

    public List<Serializable> allKeys(String partitionName) throws ObjectStoreException
    {
        // FIXME implement!
        return new ArrayList<Serializable>();
    }

    public List<String> allPartitions() throws ObjectStoreException
    {
        // FIXME implement!
        return new ArrayList<String>();
    }

    public void disposePartition(String partitionName) throws ObjectStoreException
    {
        // FIXME implement!
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    public void expire(int entryTTL, int maxEntries, String partitionName) throws ObjectStoreException
    {
        // FIXME implement!
        throw new UnsupportedOperationException("NOT IMPLEMENTED YET");
    }

    // --------- Support Methods ---------

    private DB getDatabase(Mongo mongo, String username, String password)
    {
        DB db = mongo.getDB(database);
        if (password != null)
        {
            Validate.notNull(username, "Username must not be null if password is set");
            db.authenticate(username, password.toCharArray());
        }
        return db;
    }

    protected MongoClient adaptClient(MongoClient client)
    {
        return MongoClientAdaptor.adapt(client);
    }

    public String getDatabase()
    {
        return database;
    }

    public void setDatabase(String database)
    {
        this.database = database;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
    }

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    public WriteConcern getDefaultWriteConcern()
    {
        return defaultWriteConcern;
    }

    public void setDefaultWriteConcern(WriteConcern defaultWriteConcern)
    {
        this.defaultWriteConcern = defaultWriteConcern;
    }
}
