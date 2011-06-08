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


import com.mongodb.DBObject;

import java.util.List;

/**
 * @author flbulgarelli
 */
public interface MongoClient
{

    List<String> listCollections();

    boolean existsCollection(String name);

    void dropCollection(String name);

    void createCollection(String name, boolean capped, Integer maxObjects, Integer size);

    void insertObject(String collection, DBObject object, WriteConcern writeConcern);

    void updateObject(String collection,
                      DBObject query,
                      DBObject object,
                      boolean upsert,
                      WriteConcern writeConcern);

    void saveObject(String collection, DBObject object, WriteConcern writeConcern);

    void removeObject(String collection, DBObject query);

    DBObject mapReduceObjects(String collection, String mapFunction, String reduceFunction);

    long countObjects(String collection, DBObject query);

    Iterable<DBObject> findObjects(String collection, DBObject query, DBObject fields);

    Iterable<DBObject> findOneObject(String collection, DBObject query, DBObject fields);

    void createIndex(String collection, DBObject keys);

    void dropIndex(String collection, String name);

}
