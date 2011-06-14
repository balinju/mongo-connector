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

import static org.junit.Assert.*;

import org.mule.api.lifecycle.InitialisationException;
import org.mule.module.mongo.api.IndexOrder;
import org.mule.module.mongo.api.MongoClient;
import org.mule.module.mongo.api.WriteConcern;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoException;

import java.sql.ClientInfoStatus;
import java.util.Iterator;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

/**
 * Integration test for the Connector
 * 
 * @author flbulgarelli
 */
@RunWith(Theories.class)
public class MongoTestDriver
{
    private static final String MAIN_COLLECTION = "aCollection";
    private static final String ANOTHER_COLLECTION = "anotherCollection";
    private MongoCloudConnector connector;

    /**
     * Tests methods in this test class assume that there is always a collection
     * {@link #MAIN_COLLECTION} available
     * 
     * @throws InitialisationException
     */
    @Before
    public void setup() throws InitialisationException
    {
        connector = new MongoCloudConnector();
        connector.setDatabase("mongo-connector-test");
        connector.setHost("127.0.0.1");
        connector.setPort(27017);
        connector.initialise();
        connector.createCollection(MAIN_COLLECTION, false, 100, 1000);
    }

    /**
     * Deletes the {@link #MAIN_COLLECTION}
     */
    @After
    public void tearDown()
    {
        connector.dropCollection(MAIN_COLLECTION);
        connector.dropCollection(ANOTHER_COLLECTION);
    }

    /**
     * Tests that a collection can be created, having side effects on
     * {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void createCollection() throws Exception
    {
        int originalSize = connector.listCollections().size();
        connector.createCollection(ANOTHER_COLLECTION, false, 100, 1000);
        assertTrue(connector.existsCollection(ANOTHER_COLLECTION));
        assertTrue(connector.listCollections().contains(ANOTHER_COLLECTION));
        assertEquals(originalSize + 1, connector.listCollections().size());
    }

    /**
     * Tests that a collection can not be created if it already exists
     */
    @Test(expected = MongoException.class)
    public void createCollectionAlreadyExists() throws Exception
    {
        connector.createCollection(MAIN_COLLECTION, false, 100, 1000);
    }

    /**
     * Tests that a collection can be dropped, having side effects on
     * {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void dropCollection() throws Exception
    {
        int originalSize = connector.listCollections().size();

        connector.dropCollection(MAIN_COLLECTION);

        assertFalse(connector.existsCollection(MAIN_COLLECTION));
        assertFalse(connector.listCollections().contains(MAIN_COLLECTION));

        assertEquals(originalSize - 1, connector.listCollections().size());
    }

    /**
     * Tests that a collection can be dropped even if it does not exists
     */
    @Test
    public void dropCollectionInexistent() throws Exception
    {
        connector.dropCollection(ANOTHER_COLLECTION);
    }

    /**
     * Tests that a collection can be dropped, even if it has elements, having side
     * effects on {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void dropCollectionWithElements() throws Exception
    {
        BasicDBObject o = new BasicDBObject();
        connector.insertObject(MAIN_COLLECTION, o, WriteConcern.NORMAL);
        connector.dropCollection(MAIN_COLLECTION);
        assertFalse(connector.existsCollection(MAIN_COLLECTION));
    }

    /**
     * Tests that an object can be created, impacting in the number of objects in the
     * database
     */
    @Test
    public void createObject() throws Exception
    {
        connector.insertObject(MAIN_COLLECTION, acmeEmployee(), WriteConcern.NORMAL);

        assertEquals(1, connector.countObjects(MAIN_COLLECTION, acmeQuery()));
        assertNotNull(connector.findOneObject(MAIN_COLLECTION, acmeQuery(), /* TODO */null));
    }

    /**
     * Tests that an object can be removed, impacting in the number of objects in the
     * database
     */
    @Test
    public void removeObject() throws Exception
    {
        connector.insertObject(MAIN_COLLECTION, acmeEmployee(), WriteConcern.NORMAL);

        BasicDBObject query = acmeQuery();
        connector.removeObjects(MAIN_COLLECTION, query, WriteConcern.DATABASE_DEFAULT);
        assertEquals(0, connector.countObjects(MAIN_COLLECTION, query));
    }

    /**
     * Tests that objects in a collection can be properly counted with or without
     * filters
     */
    @Test
    public void countObjects() throws Exception
    {
        insertInTestDb(new BasicDBObject("x", 59));
        insertInTestDb(new BasicDBObject("x", 60));
        insertInTestDb(new BasicDBObject("x", 60));
        insertInTestDb(new BasicDBObject("x", 70));
        assertEquals(4, connector.countObjects(MAIN_COLLECTION, null));
        assertEquals(2, connector.countObjects(MAIN_COLLECTION, new BasicDBObject("x", 60)));
        assertEquals(0, connector.countObjects(MAIN_COLLECTION, new BasicDBObject("x", 36)));
    }
    
    private void insertInTestDb(DBObject o){
        connector.insertObject(MAIN_COLLECTION, o, WriteConcern.DATABASE_DEFAULT);
    }
    
    /**
     * Some output collection names
     */
    @DataPoint
    public static final String OUTPUT_COLLECTION_NAME = "anOutputCollection";
    @DataPoint
    public static final String INLINE_COLLECTION_NAME = "null";
    
    /**
     * Tests that objects can be map-reduced either in memory or in a persistent way.
     * In this test, a collection of elections results is grouped by candidate name and 
     * reduced by votes  
     */
    @Theory
    @SuppressWarnings("serial")
    public void mapReduce(String outputCollection) throws Exception
    {
        insertInTestDb(new BasicDBObject()
        {
            {
                put("city", "City1");
                put("candidate", "John");
                put("votes", 100);
            }
        });
        insertInTestDb(new BasicDBObject()
        {
            {
                put("city", "City2");
                put("candidate", "John");
                put("votes", 20);
            }
        });
        insertInTestDb(new BasicDBObject()
        {
            {
                put("city", "City3");
                put("candidate", "Mary");
                put("votes", 150);
            }
        });
        insertInTestDb(new BasicDBObject()
        {
            {
                put("city", "City2");
                put("candidate", "Mary");
                put("votes", 60);
            }
        });
        Iterable<DBObject> results = connector.mapReduceObjects(MAIN_COLLECTION,
            "function() { emit(this.candidate, this.votes) }", 
            "function(key, values) {  var sum = 0;  values.forEach(function(x){ sum += x }); return sum; } ", outputCollection);
        assertNotNull(results);
        Iterator<DBObject> iter = results.iterator();
        assertEquals(new BasicDBObject()
        {
            {
                put("_id", "John");
                put("value", 120);
            }
        }, iter.next());
        assertEquals(new BasicDBObject()
        {
            {
                put("_id", "Mary");
                put("value", 210);
            }
        }, iter.next());
        assertFalse(iter.hasNext());
    }
    
    @Test
    public void update() throws Exception
    {
        fail("Not Yet implemented");
    }

    @Test
    public void createIndex() throws Exception
    {
        assertEquals(1, connector.listIndices(MAIN_COLLECTION).size());
        connector.createIndex(MAIN_COLLECTION, "aField", IndexOrder.DESC);
        assertEquals(2, connector.listIndices(MAIN_COLLECTION).size());
    }

    private BasicDBObject acmeQuery()
    {
        BasicDBObject query = new BasicDBObject();
        query.put("company", "ACME");
        return query;
    }

    private BasicDBObject acmeEmployee()
    {
        BasicDBObject employee = new BasicDBObject();
        employee.put("name", "John");
        employee.put("surname", "Doe");
        employee.put("company", "ACME");
        return employee;
    }
    

}
