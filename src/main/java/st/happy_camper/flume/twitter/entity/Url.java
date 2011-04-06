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
public class Url {

    public static final byte[] FAMILY = Bytes.toBytes("urls");

    public static final byte[] URL = Bytes.toBytes("url");

    public static final byte[] EXPANDED_URL = Bytes.toBytes("expanded_url");

    public static final byte[] INDICES = Bytes.toBytes("indices");

    public final String url;

    public final String expandedUrl;

    public final String indices;

    /**
     * @param json
     */
    public Url(JsonNode json) {
        url = json.path("url").getTextValue();
        expandedUrl = json.path("expanded_url").getTextValue();
        indices = json.path("indices").toString();
    }

}
