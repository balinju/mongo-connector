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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;

import java.util.Collection;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang.Validate;

public class MongoClientImpl implements MongoClient
{
    private final DB db;

    public MongoClientImpl(DB db)
    {
        Validate.notNull(db);
        this.db = db;
    }

    public long countObjects(@NotNull String collection, DBObject query)
    {
        Validate.notNull(collection);
        if (query == null)
        {
            return db.getCollection(collection).count();
        }
        return db.getCollection(collection).count(query);
    }

    public void createCollection(@NotNull String collection, boolean capped, Integer maxObjects, Integer size)
    {
        Validate.notNull(collection);
        BasicDBObject options = new BasicDBObject("capped", capped);
        if (maxObjects != null)
        {
            options.put("maxObject", maxObjects);
        }
        if (size != null)
        {
            options.put("size", size);
        }
        db.createCollection(collection, options);
    }

    public void dropCollection(@NotNull  String collection)
    {
        Validate.notNull(collection);
        db.getCollection(collection).drop();
    }

    public boolean existsCollection(@NotNull String collection)
    {
        Validate.notNull(collection);
        return db.collectionExists(collection);
    }

    public Iterable<DBObject> findObjects(@NotNull String collection, DBObject query, DBObject fields)
    {
        Validate.notNull(collection);
        return db.getCollection(collection).find(query, fields);
    }

    public DBObject findOneObject(@NotNull String collection, DBObject query, DBObject fields)
    {
        Validate.notNull(collection);
        return db.getCollection(collection).findOne(query, fields);
    }

    public void insertObject(@NotNull String collection, @NotNull DBObject object, @NotNull  WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(object);
        Validate.notNull(writeConcern);
        db.getCollection(collection).insert(object, writeConcern.toMongoWriteConcern(db));
    }

    public Collection<String> listCollections()
    {
        return db.getCollectionNames();
    }

    public DBObject mapReduceObjects(String collection, String mapFunction, String reduceFunction)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void removeObjects(@NotNull String collection, DBObject query, @NotNull WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(writeConcern);
        db.getCollection(collection).remove(query != null ? query : new BasicDBObject(), writeConcern.toMongoWriteConcern(db));
    }

    public void saveObject(@NotNull String collection,
                           @NotNull DBObject object,
                           @NotNull WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(object);
        Validate.notNull(writeConcern);
        db.getCollection(collection).save(object, writeConcern.toMongoWriteConcern(db));
    }

    public void updateObject(@NotNull String collection,
                             DBObject query,
                             DBObject object,
                             boolean upsert,
                             boolean multi, 
                             WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        db.getCollection(collection).update(query, object, upsert, multi,
            writeConcern.toMongoWriteConcern(db));

    }

    public void createIndex(String collection, String field, IndexOrder order)
    {
        db.getCollection(collection).createIndex(new BasicDBObject(field, order.getValue()));
    }

    public void dropIndex(String collection, String name)
    {
        db.getCollection(collection).dropIndex(name);
    }

    public Collection<DBObject> listIndices(String collection)
    {
        return db.getCollection(collection).getIndexInfo();
    }

}
