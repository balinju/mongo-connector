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
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import java.util.List;

import org.apache.commons.lang.Validate;

public class MongoClientImpl implements MongoClient
{
    private final DB db;

    public MongoClientImpl(DB db)
    {
        Validate.notNull(db);
        this.db = db;
    }

    public long countObjects(String collection, DBObject query)
    {
        if (query == null)
        {
            return db.getCollection(collection).count();
        }
        return db.getCollection(collection).count(query);
    }

    public void createCollection(String name, boolean capped, Integer maxObjects, Integer size)
    {
        // TODO Auto-generated method stub

    }

    public void createIndex(String collection, DBObject keys)
    {
        // TODO Auto-generated method stub

    }

    public void dropCollection(String name)
    {
        // TODO Auto-generated method stub

    }

    public void dropIndex(String collection, String name)
    {
        // TODO Auto-generated method stub

    }

    public boolean existsCollection(String name)
    {
        // TODO Auto-generated method stub
        return false;
    }

    public Iterable<DBObject> findObjects(String collection, DBObject query, DBObject fields)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public Iterable<DBObject> findOneObject(String collection, DBObject query, DBObject fields)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void insertObject(String collection, DBObject object, WriteConcern writeConcern)
    {
        // TODO Auto-generated method stub

    }

    public List<String> listCollections()
    {
        // TODO Auto-generated method stub
        return null;
    }

    public DBObject mapReduceObjects(String collection, String mapFunction, String reduceFunction)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void removeObject(String collection, DBObject query)
    {
        // TODO Auto-generated method stub

    }

    public void saveObject(String collection, DBObject object, WriteConcern writeConcern)
    {
        // TODO Auto-generated method stub

    }

    public void updateObject(String collection,
                             DBObject query,
                             DBObject object,
                             boolean upsert,
                             WriteConcern writeConcern)
    {
        // TODO Auto-generated method stub

    }

}
