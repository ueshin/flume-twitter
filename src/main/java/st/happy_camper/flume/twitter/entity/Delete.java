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
public class Delete {

    public static final byte[] FAMILY = Bytes.toBytes("delete");

    public static final byte[] ID = Bytes.toBytes("id");

    public static final byte[] USER_ID = Bytes.toBytes("user_id");

    public final long id;

    public final long userId;

    /**
     * @param json
     */
    public Delete(JsonNode json) {
        id = json.path("delete").path("status").path("id").getLongValue();
        userId = json.path("delete").path("status").path("user_id").getLongValue();
    }

}
