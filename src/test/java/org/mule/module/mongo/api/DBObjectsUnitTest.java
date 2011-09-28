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

import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DBObjectsUnitTest {

    @Test
    public void fromNull() throws Exception {
        assertNull(DBObjects.from(null));
    }

    @SuppressWarnings("serial")
    @Test
    public void fromMap() throws Exception {
        DBObject map = DBObjects.from(new HashMap<String, Object>() {
            {
                put("key1", 4);
                put("key2", Collections.singletonMap("key3", 9));
            }
        });
        assertEquals(4, map.get("key1"));
        assertThat(map.get("key2"), instanceOf(Map.class));
    }

    @Test
    public void fromMapWithId() throws Exception {
        DBObject o = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "John");
                put("surname", "Doe");
                put("age", 35);
                put("_id", 500);
            }
        });
        assertEquals("John", o.get("name"));
        assertEquals(500, o.get("_id"));
    }

    @Test
    public void fromMapWithObjectId() throws Exception {
        DBObject o = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "John");
                put("surname", "Doe");
                put("age", 35);
                put("_id", "4df7b8e8663b85b105725d34");
            }
        });
        assertEquals("John", o.get("name"));
        assertEquals(new ObjectId("4df7b8e8663b85b105725d34"), o.get("_id"));
    }

    @Test
    public void fromMapWithNestedObject() throws Exception {
        final DBObject cat = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "Garfield");
            }
        });
        DBObject o = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "Jon");
                put("surname", "Arbuckle");
                put("cat", cat);
            }
        });
        assertEquals("Jon", o.get("name"));
        assertEquals("Arbuckle", o.get("surname"));
        assertThat(o.get("cat"), instanceOf(DBObject.class));
        assertEquals("Garfield", ((DBObject) o.get("cat")).get("name"));
    }

    @Test
    public void fromMapWithNestedList() throws Exception {
        final DBObject garfield = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "Garfield");
            }
        });
        final DBObject oddie = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "Oddie");
            }
        });
        DBObject o = DBObjects.from(new HashMap<String, Object>() {
            {
                put("name", "Jon");
                put("surname", "Arbuckle");
                put("pets", Arrays.asList(garfield, oddie));
            }
        });
        assertEquals("Jon", o.get("name"));
        assertEquals("Arbuckle", o.get("surname"));
        assertThat(o.get("pets"), instanceOf(List.class));
        assertTrue(((List<?>) o.get("pets")).get(0) instanceof DBObject);
    }
}