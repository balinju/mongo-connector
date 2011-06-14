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
import static org.mockito.Matchers.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import com.mongodb.DBObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.types.ObjectId;
import org.junit.Test;

public class DBObjectsUnitTest
{

    @Test
    public void fromNull() throws Exception
    {
        assertNull(DBObjects.from(null));
    }

    @SuppressWarnings("serial")
    @Test
    public void testFromMap() throws Exception
    {
        DBObject map = DBObjects.from(new HashMap<String, Object>()
        {
            {
                put("key1", 4);
                put("key2", Collections.singletonMap("key3", 9));
            }
        });
        assertEquals(4, map.get("key1"));
        assertThat(map.get("key2"), instanceOf(Map.class));
    }

    @Test
    public void testFromJson() throws Exception
    {
        DBObject o = DBObjects.from("{ \"name\": \"John\", \"surname\": \"Doe\", \"age\": 35}");
        assertEquals("John", o.get("name"));
        assertEquals("Doe", o.get("surname"));
        assertEquals(35, o.get("age"));
    }

    @Test
    public void testFromJsonWithId() throws Exception
    {
        DBObject o = DBObjects.from("{ \"name\": \"John\", \"surname\": \"Doe\", \"age\": 35, \"_id\": 500}");
        assertEquals("John", o.get("name"));
        assertEquals(500, o.get("_id"));
    }

    @Test
    public void testFromJsonWIthObjectId() throws Exception
    {
        DBObject o = DBObjects.from("{ \"name\": \"John\", \"surname\": \"Doe\", \"age\": 35, \"_id\": \"ObjectId(4df7b8e8663b85b105725d34)\"}");
        assertEquals("John", o.get("name"));
        assertEquals(new ObjectId("4df7b8e8663b85b105725d34"), o.get("_id"));
    }

    @Test
    public void testFromJsonWithNestedObjects() throws Exception
    {
        DBObject o = DBObjects.from("{ \"name\": \"Jon\", \"surname\": \"Arbuckle\", \"cat\" : { \"name\" : \"Garfield\" }}");
        assertEquals("Jon", o.get("name"));
        assertEquals("Arbuckle", o.get("surname"));
        assertThat(o.get("cat"), instanceOf(DBObject.class));
        assertEquals("Garfield", ((DBObject) o.get("cat")).get("name"));
    }

    @Test
    public void testFromJsonWithList() throws Exception
    {
        DBObject o = DBObjects.from("{ \"name\": \"Jon\", \"surname\": \"Arbuckle\", \"pets\" : [ { \"name\" : \"Garfield\" } , {\"name\": \"Oddie\"} ] }");
        assertEquals("Jon", o.get("name"));
        assertEquals("Arbuckle", o.get("surname"));
        assertThat(o.get("pets"), instanceOf(List.class));
        assertTrue(((List<?>) o.get("pets")).get(0) instanceof DBObject);
    }
}
