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
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;
import com.mongodb.MapReduceCommand.OutputType;

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
            return openSession().getCollection(collection).count();
        }
        return openSession().getCollection(collection).count(query);
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
        openSession().createCollection(collection, options);
    }

    public void dropCollection(@NotNull String collection)
    {
        Validate.notNull(collection);
        openSession().getCollection(collection).drop();
    }

    public boolean existsCollection(@NotNull String collection)
    {
        Validate.notNull(collection);
        return openSession().collectionExists(collection);
    }

    public Iterable<DBObject> findObjects(@NotNull String collection, DBObject query, DBObject fields)
    {
        Validate.notNull(collection);
        return openSession().getCollection(collection).find(query, fields);
    }

    public DBObject findOneObject(@NotNull String collection, DBObject query, DBObject fields)
    {
        Validate.notNull(collection);
        DBObject element = openSession().getCollection(collection).findOne(query, fields);
        if (element == null)
        {
            throw new MongoException("No object found for query " + query);
        }
        return element;
    }

    public void insertObject(@NotNull String collection,
                             @NotNull DBObject object,
                             @NotNull WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(object);
        Validate.notNull(writeConcern);
        openSession().getCollection(collection).insert(object,
            writeConcern.toMongoWriteConcern(openSession()));
    }

    public Collection<String> listCollections()
    {
        return openSession().getCollectionNames();
    }

    public Iterable<DBObject> mapReduceObjects(@NotNull String collection,
                                               @NotNull String mapFunction,
                                               @NotNull String reduceFunction,
                                               String outputCollection)
    {
        Validate.notNull(collection);
        Validate.notEmpty(mapFunction);
        Validate.notEmpty(reduceFunction);
        return openSession().getCollection(collection).mapReduce(mapFunction, reduceFunction,
            outputCollection, outputTypeFor(outputCollection), null).results();
    }

    private OutputType outputTypeFor(String outputCollection)
    {
        return outputCollection != null ? OutputType.REPLACE : OutputType.INLINE;
    }

    public void removeObjects(@NotNull String collection, DBObject query, @NotNull WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(writeConcern);
        openSession().getCollection(collection).remove(query != null ? query : new BasicDBObject(),
            writeConcern.toMongoWriteConcern(openSession()));
    }

    public void saveObject(@NotNull String collection,
                           @NotNull DBObject object,
                           @NotNull WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(object);
        Validate.notNull(writeConcern);
        openSession().getCollection(collection).save(object, writeConcern.toMongoWriteConcern(openSession()));
    }

    public void updateObjects(@NotNull String collection,
                              DBObject query,
                              DBObject object,
                              boolean upsert,
                              boolean multi,
                              WriteConcern writeConcern)
    {
        Validate.notNull(collection);
        Validate.notNull(writeConcern);
        openSession().getCollection(collection).update(query, object, upsert, multi,
            writeConcern.toMongoWriteConcern(openSession()));

    }

    public void createIndex(String collection, String field, IndexOrder order)
    {
        openSession().getCollection(collection).createIndex(new BasicDBObject(field, order.getValue()));
    }

    public void dropIndex(String collection, String name)
    {
        openSession().getCollection(collection).dropIndex(name);
    }

    public Collection<DBObject> listIndices(String collection)
    {
        return openSession().getCollection(collection).getIndexInfo();
    }

    /**
     * Gets the DB objects, ensuring that a consistent request is in progress.
     * Consistent requests are never ended, they end when the current thread finishes
     */
    private DB openSession()
    {
        db.requestStart();
        db.requestEnsureConnection();
        return db;
    }

    public DB getDb()
    {
        return db;
    }

}
