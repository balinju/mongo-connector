/**
 * Mule MongoDB Cloud Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.module.mongo;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.mule.module.mongo.api.MongoClientImpl;

import com.mongodb.DB;
import com.mongodb.Mongo;

import java.net.UnknownHostException;

import org.junit.Test;

public class MongoCloudConnectorCacheTest
{
    @Test
    public void shareMongo() throws Exception
    {
        MongoCloudConnector connector1 = new DisconnectedMongoCloudConnector();
        connector1.setHost("127.0.0.1");
        connector1.setPort(27017);
        connector1.setDatabase("mongo-connector-test");
        connector1.initialise();
        
        MongoCloudConnector connector2 = new DisconnectedMongoCloudConnector();
        connector2.setHost("127.0.0.1");
        connector2.setPort(27017);
        connector2.setDatabase("another-database");
        connector2.initialise();
        
        MongoCloudConnector connector3 = new DisconnectedMongoCloudConnector();
        connector3.setHost("127.0.0.1");
        connector3.setPort(27018);
        connector3.setDatabase("another-database");
        connector3.initialise();


        assertSame(getUnderlyingMongo(connector2), getUnderlyingMongo(connector1));
        assertNotSame(getUnderlyingMongo(connector3), getUnderlyingMongo(connector1));
    }
    
    @Test
    public void shareMongoAfterGC() throws Exception
    {
        MongoCloudConnector connector1 = new DisconnectedMongoCloudConnector();
        connector1.setHost("127.0.0.1");
        connector1.setPort(15000);
        connector1.setDatabase("mongo-connector-test");
        connector1.initialise();
        
        Mongo mongo1 = getUnderlyingMongo(connector1);
        
        connector1 = null;
        System.gc();
        
        MongoCloudConnector connector2 = new DisconnectedMongoCloudConnector();
        connector2.setHost("127.0.0.1");
        connector2.setPort(15000);
        connector2.setDatabase("another-database");
        connector2.initialise();
        
        assertNotSame(getUnderlyingMongo(connector2), mongo1);
    }

    private Mongo getUnderlyingMongo(MongoCloudConnector c)
    {
        return ((MongoClientImpl) c.getClient()).getDb().getMongo();
    }

    private final class DisconnectedMongoCloudConnector extends MongoCloudConnector
    {
        @Override
        protected Mongo newMongo() throws UnknownHostException
        {
            Mongo mongo = mock(Mongo.class);
            DB db = mock(DB.class);
            when(mongo.getDB(anyString())).thenReturn(db);
            when(db.getMongo()).thenReturn(mongo);
            return mongo;
        }
    }

}
