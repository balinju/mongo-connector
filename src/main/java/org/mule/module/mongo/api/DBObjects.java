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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.Map;

public final class DBObjects
{
    private DBObjects()
    {
    }

    public static DBObject from(Map<String, Object> map)
    {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.putAll(map);
        return dbObject;
    }

    @SuppressWarnings("unchecked")
    public static DBObject from(Object o)
    {
        if (o == null)
        {
            return null;
        }
        if (o instanceof Map<?, ?>)
        {
            return from((Map<String, Object>) o);
        }
        if (o instanceof DBObject)
        {
            return (DBObject) o;
        }
        throw new IllegalArgumentException("Unsupported object type " + o);
    }

}
