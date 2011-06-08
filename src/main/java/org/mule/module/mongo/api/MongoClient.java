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

}
