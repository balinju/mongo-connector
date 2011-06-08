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

import org.mule.module.mongo.api.MongoClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoTestDriver
{
    private static final String MAIN_COLLECTION = "aCollection";
    private static final String ANOTHER_COLLECTION = "anotherCollection";
    private MongoCloudConnector connector;

    /**
     * Tests methods in this test class assume that there is always a collection
     * {@link #MAIN_COLLECTION} available
     */
    @Before
    public void setup()
    {
        connector.createCollection(MAIN_COLLECTION, false, 100, 1000);
    }

    /**
     * Deletes the {@link #MAIN_COLLECTION}
     */
    @After
    public void tearDown()
    {
        connector.dropCollection(MAIN_COLLECTION);
    }

    /**
     * Tests that a collection can be created, having side effects on
     * {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void createCollection() throws Exception
    {
        connector.createCollection(ANOTHER_COLLECTION, false, 100, 1000);
        assertTrue(connector.existsCollection(ANOTHER_COLLECTION));
        assertTrue(connector.listCollections().contains(ANOTHER_COLLECTION));
        assertEquals(2, connector.listCollections().size());
    }

    /**
     * Tests that a collection can be dropped, having side effects on
     * {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void dropCollection() throws Exception
    {
        connector.createCollection(ANOTHER_COLLECTION, false, 100, 1000);
        connector.dropCollection(ANOTHER_COLLECTION);
        assertFalse(connector.existsCollection(ANOTHER_COLLECTION));
        assertFalse(connector.listCollections().contains(ANOTHER_COLLECTION));
        assertEquals(1, connector.listCollections().size());
    }

    /**
     * Tests that a collection can be dropped, even if it has elements, having side
     * effects on {@link MongoCloudConnector#existsCollection(String)} and
     * {@link MongoClient#listCollections()}
     */
    @Test
    public void dropCollectionWithElements() throws Exception
    {
        fail("Not yet implemented");
    }

}
