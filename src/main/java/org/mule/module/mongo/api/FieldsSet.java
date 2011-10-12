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

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public final class FieldsSet
{
    private FieldsSet()
    {
    }

    public static DBObject from(List<String> fieldsList)
    {
        if (fieldsList == null)
        {
            return null;
        }

        BasicDBObject o = new BasicDBObject();
        for (String s : fieldsList)
        {
            o.put(s, 1);
        }
        return o;
    }

}
