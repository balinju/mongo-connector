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

import java.util.Collection;

import javax.validation.constraints.NotNull;

/**
 * @author flbulgarelli
 */
public interface MongoClient
{

    Collection<String> listCollections();

    boolean existsCollection(@NotNull String name);

    void dropCollection(@NotNull String name);

    void createCollection(@NotNull String name, Boolean capped, Integer maxObjects, Integer size);

    void insertObject(@NotNull String collection, @NotNull DBObject object, @NotNull WriteConcern writeConcern);

    void updateObject(@NotNull String collection,
                      DBObject query,
                      DBObject object,
                      boolean upsert,
                      WriteConcern writeConcern);

    void saveObject(String collection, DBObject object, WriteConcern writeConcern);

    void removeObjects(@NotNull String collection, DBObject query, @NotNull WriteConcern writeConcern);

    DBObject mapReduceObjects(String collection, String mapFunction, String reduceFunction);

    long countObjects(@NotNull String collection, DBObject query);

    Iterable<DBObject> findObjects(@NotNull String collection, DBObject query, DBObject fields);

    DBObject findOneObject(@NotNull String collection, DBObject query, DBObject fields);

    void createIndex(String collection, String field, IndexOrder order);

    void dropIndex(String collection, String name);

    Collection<DBObject> listIndices(String collection);

}
