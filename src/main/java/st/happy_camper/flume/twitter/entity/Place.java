/*
 * Copyright 2010 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.flume.twitter.entity;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;

/**
 * @author ueshin
 */
public class Place {

    public static final byte[] FAMILY = Bytes.toBytes("place");

    public static final byte[] ID = Bytes.toBytes("id");

    public static final byte[] NAME = Bytes.toBytes("name");

    public static final byte[] FULL_NAME = Bytes.toBytes("full_name");

    public static final byte[] URL = Bytes.toBytes("url");

    public static final byte[] PLACE_TYPE = Bytes.toBytes("place_type");

    public static final byte[] COUNTRY = Bytes.toBytes("country");

    public static final byte[] COUNTRY_CODE = Bytes.toBytes("country_code");

    public static final byte[] BOUNDING_BOX = Bytes.toBytes("bounding_box");

    public static final byte[] BOUNDING_BOX_TYPE = Bytes.toBytes("bounding_box_type");

    public static final byte[] ATTRIBUTES = Bytes.toBytes("attributes");

    public final String id;

    public final String name;

    public final String fullName;

    public final String url;

    public final String placeType;

    public final String country;

    public final String countryCode;

    public final String boundingBox;

    public final String boundingBoxType;

    public final String attributes;

    /**
     * @param path
     */
    public Place(JsonNode json) {
        id = json.path("id").getTextValue();
        name = json.path("name").getTextValue();
        fullName = json.path("full_name").getTextValue();
        url = json.path("url").getTextValue();
        placeType = json.path("place_type").getTextValue();
        country = json.path("country").getTextValue();
        countryCode = json.path("country_code").getTextValue();
        boundingBox = json.path("bounding_box").isNull() ? null : json.path("bounding_box").path("coordinates").toString();
        boundingBoxType = json.path("bounding_box").path("type").getTextValue();
        attributes = json.path("attributes").toString();
    }

}
