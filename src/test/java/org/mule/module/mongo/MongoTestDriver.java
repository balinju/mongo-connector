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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.module.mongo.api.IndexOrder;
import org.mule.module.mongo.api.MongoClient;
import org.mule.module.mongo.api.WriteConcern;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
    private MongoCloudConnector.MongoSession session;

    /**
     * Tests methods in this test class assume that there is always a collection
     * {@link #MAIN_COLLECTION} available
     * 
     * @throws InitialisationException
     */
    @Before
    public void setup() throws Exception
    {
        connector = new MongoCloudConnector();
        connector.setDatabase("mongo-connector-test");
        connector.setHost("127.0.0.1");
        connector.setPort(27017);
        session = connector.createSession("user", "pass");
        connector.createCollection(session, MAIN_COLLECTION, false, 100, 1000);
    }

    /**
     * Deletes the {@link #MAIN_COLLECTION}
     */
    @After
    public void tearDown()
    {
        connector.dropCollection(session, MAIN_COLLECTION);
        connector.dropCollection(session, ANOTHER_COLLECTION);
    }

    /**
     * Tests that a collection can be created, having side effects on
     * {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void createCollection() throws Exception
    {
        int originalSize = connector.listCollections(session).size();
        connector.createCollection(session, ANOTHER_COLLECTION, false, 100, 1000);
        assertTrue(connector.existsCollection(session, ANOTHER_COLLECTION));
        assertTrue(connector.listCollections(session).contains(ANOTHER_COLLECTION));
        assertEquals(originalSize + 1, connector.listCollections(session).size());
    }

    /**
     * Tests that a collection can not be created if it already exists
     */
    @Test(expected = MongoException.class)
    public void createCollectionAlreadyExists() throws Exception
    {
        connector.createCollection(session, MAIN_COLLECTION, false, 100, 1000);
    }

    /**
     * Tests that a collection can be dropped, having side effects on
     * {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void dropCollection() throws Exception
    {
        int originalSize = connector.listCollections(session).size();

        connector.dropCollection(session, MAIN_COLLECTION);

        assertFalse(connector.existsCollection(session, MAIN_COLLECTION));
        assertFalse(connector.listCollections(session).contains(MAIN_COLLECTION));

        assertEquals(originalSize - 1, connector.listCollections(session).size());
    }

    /**
     * Tests that a collection can be dropped even if it does not exists
     */
    @Test
    public void dropCollectionInexistent() throws Exception
    {
        connector.dropCollection(session, ANOTHER_COLLECTION);
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
        connector.insertObject(session, MAIN_COLLECTION, o, WriteConcern.NORMAL);
        connector.dropCollection(session, MAIN_COLLECTION);
        assertFalse(connector.existsCollection(session, MAIN_COLLECTION));
    }

    /**
     * Tests that an object can be created, impacting in the number of objects in the
     * database
     */
    @Test
    public void createObject() throws Exception
    {
        connector.insertObject(session, MAIN_COLLECTION, acmeEmployee(), WriteConcern.NORMAL);

        assertEquals(1, connector.countObjects(session, MAIN_COLLECTION, acmeQuery()));
        DBObject employee = connector.findOneObject(session, MAIN_COLLECTION, acmeQuery(), Arrays.asList("name"));
        assertNotNull(employee);
        assertEquals("John", employee.get("name"));
        assertNull(employee.get("company"));
    }

    /**
     * Tests that an exception is thrown if no object that matches a query is found
     */
    @Test(expected = MongoException.class)
    public void findOneObjectNotExists() throws Exception
    {
        connector.findOneObject(session, MAIN_COLLECTION, acmeQuery(), null);
    }

    /**
     * Tests that an object can be removed, impacting in the number of objects in the
     * database
     */
    @Test
    public void removeObject() throws Exception
    {
        connector.insertObject(session, MAIN_COLLECTION, acmeEmployee(), WriteConcern.NORMAL);

        BasicDBObject query = acmeQuery();
        connector.removeObjects(session, MAIN_COLLECTION, query, WriteConcern.DATABASE_DEFAULT);
        assertEquals(0, connector.countObjects(session, MAIN_COLLECTION, query));
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
        assertEquals(4, connector.countObjects(session, MAIN_COLLECTION, null));
        assertEquals(2, connector.countObjects(session, MAIN_COLLECTION, new BasicDBObject("x", 60)));
        assertEquals(0, connector.countObjects(session, MAIN_COLLECTION, new BasicDBObject("x", 36)));
    }

    private void insertInTestDb(DBObject o)
    {
        connector.insertObject(session, MAIN_COLLECTION, o, WriteConcern.DATABASE_DEFAULT);
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
     * In this test, a collection of elections results is grouped by candidate name
     * and reduced by votes
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
        Iterable<DBObject> results = connector.mapReduceObjects(session, MAIN_COLLECTION,
            "function() { emit(this.candidate, this.votes) }",
            "function(key, values) { return values.reduce(function(a, e){ return a + e });  } ",
            outputCollection);
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
    public void updateMulti() throws Exception
    {
        insertInTestDb(new BasicDBObject("x", 50));
        insertInTestDb(new BasicDBObject("x", 60));
        insertInTestDb(new BasicDBObject("x", 60));
        insertInTestDb(new BasicDBObject("x", 70));
        connector.updateObjects(session, MAIN_COLLECTION, new BasicDBObject("x", new BasicDBObject("$gt", 55)),
            new BasicDBObject("$inc", new BasicDBObject("x", 2)), false, true, WriteConcern.DATABASE_DEFAULT);

        Iterator<DBObject> iter = connector.findObjects(session, MAIN_COLLECTION, null, null).iterator();
        assertEquals(50, iter.next().get("x"));
        assertEquals(62, iter.next().get("x"));
        assertEquals(62, iter.next().get("x"));
        assertEquals(72, iter.next().get("x"));
    }

    @Test
    public void updateSingle() throws Exception
    {
        insertInTestDb(new BasicDBObject("x", 50));
        insertInTestDb(new BasicDBObject("x", 60));
        insertInTestDb(new BasicDBObject("x", 60));
        connector.updateObjects(session, MAIN_COLLECTION, new BasicDBObject("x", new BasicDBObject("$gt", 55)),
            new BasicDBObject("$inc", new BasicDBObject("x", 2)), false, false, WriteConcern.DATABASE_DEFAULT);

        Iterator<DBObject> iter = connector.findObjects(session, MAIN_COLLECTION, null, null).iterator();
        assertEquals(50, iter.next().get("x"));
        assertEquals(62, iter.next().get("x"));
        assertEquals(60, iter.next().get("x"));
    }

    @Test
    public void createIndex() throws Exception
    {
        assertEquals(1, connector.listIndices(session, MAIN_COLLECTION).size());
        connector.createIndex(session, MAIN_COLLECTION, "aField", IndexOrder.DESC);
        assertEquals(2, connector.listIndices(session, MAIN_COLLECTION).size());
    }

    @Test
    public void testCreateAndGetFile() throws Exception
    {
        DBObject file = connector.createFileFromPayload(session, new ByteArrayInputStream("hello world".getBytes()),
                "testFile.txt", "text/plain", new BasicDBObject("foo", "bar"));
        try
        {
            assertEquals("testFile.txt", file.get("filename"));
            assertEquals("text/plain", file.get("contentType"));
            assertEquals("bar", ((DBObject) file.get("metadata")).get("foo"));
            
            InputStream in = connector.getFileContent(session, filenameQuery("testFile.txt"));
            assertEquals("hello world", new Scanner(in).nextLine());
        }
        finally
        {
            connector.removeFiles(session, filenameQuery("testFile.txt"));
        }
    }
    
    @Test
    public void testCreateAndListFile() throws Exception
    {
        connector.createFileFromPayload(session, "hello world".getBytes(), "testFile.txt", null,
                null);
        try
        {
            Iterator<DBObject> iter = connector.listFiles(session, filenameQuery("testFile.txt")).iterator();
            assertTrue(iter.hasNext());
            iter.next();
            assertFalse(iter.hasNext());
            
            iter = connector.findFiles(session, filenameQuery("testFile.txt")).iterator();
            assertTrue(iter.hasNext());
            iter.next();
            assertFalse(iter.hasNext());
        }
        finally
        {
            connector.removeFiles(session, filenameQuery("testFile.txt"));
        }
    }

    private BasicDBObject filenameQuery(String filename)
    {
        return new BasicDBObject("filename", filename);
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