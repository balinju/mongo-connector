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

public enum IndexOrder
{
    ASC(1), DESC(-1);

    private final int value;

    private IndexOrder(int value)
    {
        this.value = value;
    }

    public int getValue()
    {
        return value;
    }

}
