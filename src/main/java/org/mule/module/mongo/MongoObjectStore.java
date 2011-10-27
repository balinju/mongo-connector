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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.bson.types.ObjectId;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Module;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;
import org.mule.api.store.ObjectDoesNotExistException;
import org.mule.api.store.ObjectStoreException;
import org.mule.api.store.PartitionableExpirableObjectStore;
import org.mule.module.mongo.api.IndexOrder;
import org.mule.module.mongo.api.MongoClient;
import org.mule.module.mongo.api.MongoClientImpl;
import org.mule.module.mongo.api.WriteConcern;
import org.mule.util.SerializationUtils;
import org.springframework.util.DigestUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.QueryBuilder;

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

    private static final String ID_FIELD = "_id";
    private static final String KEY_FIELD = "key";
    private static final String TIMESTAMP_FIELD = "timestamp";
    private static final String VALUE_FIELD = "value";
    private static final List<String> NO_FIELD_LIST = Collections.emptyList();

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
     * The username used to connect to the Mongo server
     */
    @Configurable
    @Optional
    @Default("")
    private String username;

    /**
     * The password used to connect to the Mongo server
     */
    @Configurable
    @Optional
    @Default("")
    private String password;

    /**
     * The default concern to use to when writing to Mongo
     */
    @Configurable
    @Optional
    @Default("DATABASE_DEFAULT")
    private WriteConcern writeConcern;

    private MongoClient mongoClient;

    @PostConstruct
    public void initialize() throws UnknownHostException
    {
        DB db = new Mongo(host, port).getDB(database);
        if (StringUtils.isNotEmpty(password))
        {
            Validate.notEmpty(username, "Username must not be empty if password is set");
            db.authenticate(username, password.toCharArray());
        }

        mongoClient = new MongoClientImpl(db);
    }

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
        ObjectId objectId = getObjectIdFromKey(key);
        DBObject query = getQueryForObjectId(objectId);
        String collection = getCollectionName(partitionName);
        return mongoClient.findObjects(collection, query, NO_FIELD_LIST).iterator().hasNext();
    }

    public List<Serializable> allKeys(String partitionName) throws ObjectStoreException
    {
        String collection = getCollectionName(partitionName);
        Iterable<DBObject> keyObjects = mongoClient.findObjects(collection, new BasicDBObject(),
            Arrays.asList(KEY_FIELD));

        ArrayList<Serializable> results = new ArrayList<Serializable>();
        for (DBObject keyObject : keyObjects)
        {
            results.add((Serializable) SerializationUtils.deserialize((byte[]) keyObject.get(KEY_FIELD)));
        }
        return results;
    }

    public List<String> allPartitions() throws ObjectStoreException
    {
        ArrayList<String> results = new ArrayList<String>();

        for (String collection : mongoClient.listCollections())
        {
            if (isPartition(collection))
            {
                results.add(getPartitionName(collection));
            }
        }

        return results;
    }

    public void store(Serializable key, Serializable value, String partitionName) throws ObjectStoreException
    {
        String collection = getCollectionName(partitionName);
        if (!mongoClient.existsCollection(collection))
        {
            mongoClient.createCollection(collection, false, null, null);
            mongoClient.createIndex(collection, TIMESTAMP_FIELD, IndexOrder.ASC);
        }

        byte[] keyAsBytes = SerializationUtils.serialize(key);
        ObjectId objectId = getObjectIdFromKey(keyAsBytes);
        DBObject query = getQueryForObjectId(objectId);
        DBObject dbObject = new BasicDBObject();
        dbObject.put(ID_FIELD, objectId);
        dbObject.put(TIMESTAMP_FIELD, System.currentTimeMillis());
        dbObject.put(KEY_FIELD, keyAsBytes);
        dbObject.put(VALUE_FIELD, SerializationUtils.serialize(value));
        mongoClient.updateObjects(collection, query, dbObject, true, false, getWriteConcern());
    }

    public Serializable retrieve(Serializable key, String partitionName) throws ObjectStoreException
    {
        String collection = getCollectionName(partitionName);
        ObjectId objectId = getObjectIdFromKey(key);
        DBObject query = getQueryForObjectId(objectId);
        return retrieveSerializedObject(collection, query);
    }

    public Serializable remove(Serializable key, String partitionName) throws ObjectStoreException
    {
        String collection = getCollectionName(partitionName);
        ObjectId objectId = getObjectIdFromKey(key);
        DBObject query = getQueryForObjectId(objectId);

        Serializable result = retrieveSerializedObject(collection, query);
        mongoClient.removeObjects(collection, query, getWriteConcern());
        return result;
    }

    public void disposePartition(String partitionName) throws ObjectStoreException
    {
        String collection = getCollectionName(partitionName);
        mongoClient.dropCollection(collection);
    }

    public void expire(int entryTTL, int ignored_maxEntries, String partitionName)
        throws ObjectStoreException
    {
        String collection = getCollectionName(partitionName);
        long expireAt = System.currentTimeMillis() - entryTTL;
        DBObject query = QueryBuilder.start(TIMESTAMP_FIELD).lessThan(expireAt).get();
        mongoClient.removeObjects(collection, query, getWriteConcern());
    }

    // --------- Java Accessor Festival ---------

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

    public String getUsername()
    {
        return username;
    }

    public void setUsername(String username)
    {
        this.username = username;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public WriteConcern getWriteConcern()
    {
        return writeConcern;
    }

    public void setWriteConcern(WriteConcern writeConcern)
    {
        this.writeConcern = writeConcern;
    }

    // --------- Support Methods ---------

    private String getCollectionName(String partitionName)
    {
        return OBJECTSTORE_COLLECTION_PREFIX + partitionName;
    }

    private String getPartitionName(String collectionName)
    {
        return StringUtils.substringAfter(collectionName, OBJECTSTORE_COLLECTION_PREFIX);
    }

    private boolean isPartition(String collectionName)
    {
        return StringUtils.startsWith(collectionName, OBJECTSTORE_COLLECTION_PREFIX);
    }

    private ObjectId getObjectIdFromKey(Serializable key)
    {
        byte[] keyAsBytes = SerializationUtils.serialize(key);
        return getObjectIdFromKey(keyAsBytes);
    }

    private ObjectId getObjectIdFromKey(byte[] keyAsBytes)
    {
        // hash the key and combine the resulting 16 bytes down to 12
        byte[] md5Digest = DigestUtils.md5Digest(keyAsBytes);
        byte[] id = ArrayUtils.subarray(md5Digest, 0, 12);
        for (int i = 0; i < 4; i++)
        {
            id[i * 3] = (byte) (id[i * 3] ^ md5Digest[12 + i]);
        }
        ObjectId objectId = new ObjectId(id);
        return objectId;
    }

    private DBObject getQueryForObjectId(ObjectId objectId)
    {
        return new BasicDBObject(ID_FIELD, objectId);
    }

    private Serializable retrieveSerializedObject(String collection, DBObject query)
        throws ObjectDoesNotExistException
    {
        Iterator<DBObject> iterator = mongoClient.findObjects(collection, query, Arrays.asList(VALUE_FIELD))
            .iterator();

        if (!iterator.hasNext())
        {
            throw new ObjectDoesNotExistException();
        }

        DBObject dbObject = iterator.next();
        return (Serializable) SerializationUtils.deserialize((byte[]) dbObject.get(VALUE_FIELD));
    }
}
