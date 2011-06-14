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
 * Integration test for the Connector when Authenticated.
 * This test is only meaningful when server has being started with 
 * --auth argument, and a user has being created:
 * 
 * <pre>db.addUser(username, password)</pre>
 * 
 * @author flbulgarelli
 */
public class MongoAuthenticationTestDriver
{
    private MongoCloudConnector connector;

    /**
     * Setups an athenticated connector
     */
    @Before
    public void setup() throws InitialisationException
    {
        connector = new MongoCloudConnector();
        connector.setDatabase("mongo-connector-test");
        connector.setHost("127.0.0.1");
        connector.setPort(27017);
        connector.setPassword("1234");
        connector.setUsername("foobar");
        connector.initialise();
    }

    @Test
    public void createCollection() throws Exception
    {
        assertNotNull(connector.listCollections());
    }

}
