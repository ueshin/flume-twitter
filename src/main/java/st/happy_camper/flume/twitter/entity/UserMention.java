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
public class UserMention {

    public static final byte[] FAMILY = Bytes.toBytes("user_mentions");

    public static final byte[] ID = Bytes.toBytes("id");

    public static final byte[] NAME = Bytes.toBytes("name");

    public static final byte[] SCREEN_NAME = Bytes.toBytes("screen_name");

    public static final byte[] INDICES = Bytes.toBytes("indices");

    public final long id;

    public final String name;

    public final String screenName;

    public final String indices;

    /**
     * @param json
     */
    public UserMention(JsonNode json) {
        id = json.path("id").getLongValue();
        name = json.path("name").getTextValue();
        screenName = json.path("screen_name").getTextValue();
        indices = json.path("indices").toString();
    }

}
