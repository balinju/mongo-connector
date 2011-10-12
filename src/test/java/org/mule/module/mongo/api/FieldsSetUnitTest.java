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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

import com.mongodb.BasicDBObject;

public class FieldsSetUnitTest
{
    @Test
    public void fromEmptyList() throws Exception
    {
        assertEquals(new BasicDBObject(), FieldsSet.from(Arrays.<String> asList()));
    }

    @Test
    public void fromNonEmpty() throws Exception
    {
        assertEquals(new BasicDBObject("f1", 1), FieldsSet.from(Arrays.asList("f1")));
    }

    @Test
    public void fromNull() throws Exception
    {
        assertNull(FieldsSet.from(null));
    }

}
