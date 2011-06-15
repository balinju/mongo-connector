/**
 * Mule MongoDB Cloud Connector
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mulesoft.demo.mongo;

import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.transport.PropertyScope;
import org.mule.construct.SimpleFlowConstruct;
import org.mule.tck.FunctionalTestCase;

import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;

public class MongoFunctionalTestDriver extends FunctionalTestCase
{

    @Override
    protected String getConfigResources()
    {
        return "mule-config.xml";
    }

    public void testInsertProductJson() throws Exception
    {
        MuleEvent event = getTestEvent("");
        MuleMessage message = event.getMessage();
        message.setProperty("sku", "FX48960", PropertyScope.INBOUND);
        message.setProperty("description", "A product", PropertyScope.INBOUND);
        message.setProperty("price", "56.50", PropertyScope.INBOUND);
        message.setProperty("available", "false", PropertyScope.INBOUND);
        lookupFlowConstruct("InsertProduct").process(event);
    }

    public void testInsertProductJsonFlow() throws Exception
    {
        lookupFlowConstruct("InsertProductJson").process(
            getTestEvent("{ \"sku\" : \"AF459\", \"description\" : \"Another Product\", \"price\" : 459.05, \"available\" : true }"));
    }

    private SimpleFlowConstruct lookupFlowConstruct(final String name)
    {
        return (SimpleFlowConstruct) muleContext.getRegistry().lookupFlowConstruct(name);
    }
    
    @Override
    public void handleTimeout(long timeout, TimeUnit unit)
    {
     
    }
    
    
}

