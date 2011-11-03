/**
 * Mule Mongo Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.module.mongo;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

/**
 * Integration test for the Connector when Authenticated. This test is only
 * meaningful when server has being started with --auth argument, and a user has
 * being created:
 * 
 * <pre>
 * db.addUser(username, password)
 * </pre>
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
    public void setup() throws Exception
    {
        connector = new MongoCloudConnector();
        connector.setDatabase("mongo-connector-test");
        connector.setHost("127.0.0.1");
        connector.setPort(27017);
        connector.connect("foobar", "1234");
    }

    @Test
    public void createCollection() throws Exception
    {
        assertNotNull(connector.listCollections());
    }

}
