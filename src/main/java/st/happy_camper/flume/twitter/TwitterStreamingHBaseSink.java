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
package st.happy_camper.flume.twitter;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.IllegalSelectorException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;

/**
 * @author ueshin
 */
public class TwitterStreamingHBaseSink extends EventSink.Base {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStreamingHBaseSink.class);

    private static final byte[] STATUS = Bytes.toBytes("status");

    private static final byte[] PLACE = Bytes.toBytes("place");

    private static final byte[] USER_MENTIONS = Bytes.toBytes("user_mentions");

    private static final byte[] URLS = Bytes.toBytes("urls");

    private static final byte[] HASHTAGS = Bytes.toBytes("hashtags");

    private static final byte[] USER = Bytes.toBytes("user");

    private final Configuration conf;

    private final String tableName;

    /**
     * @param table
     * @throws IOException
     */
    public TwitterStreamingHBaseSink(Configuration conf, String tableName) {
        this.conf = conf;
        this.tableName = tableName;

        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            if(!admin.tableExists(tableName)) {
                throw new IllegalArgumentException("Table " + tableName + " not exsist.");
            }
        }
        catch(IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private HTable table;

    /*
     * (non-Javadoc)
     * @see com.cloudera.flume.core.EventSink.Base#open()
     */
    @Override
    public void open() throws IOException, InterruptedException {
        if(table == null) {
            this.table = new HTable(conf, tableName);
        }
        else {
            throw new IllegalSelectorException();
        }
    }

    /*
     * (non-Javadoc)
     * @see
     * com.cloudera.flume.core.EventSink.Base#append(com.cloudera.flume.core
     * .Event)
     */
    @Override
    public synchronized void append(Event event) throws IOException, InterruptedException {
        JsonNode json;
        try {
            json = new ObjectMapper().readTree(new ByteArrayInputStream(event.getBody()));
        }
        catch(EOFException e) {
            return;
        }

        long id = json.path("id").getLongValue();
        Put put = null;
        if(id > 0L) {
            try {
                // status
                put = new Put(makeKey(event.getTimestamp(), id), event.getTimestamp());

                put.add(STATUS, Bytes.toBytes("id"), Bytes.toBytes(id));
                put.add(STATUS, Bytes.toBytes("created_at"), Bytes.toBytes(createdAtDateFormat().parse(json.path("created_at").getTextValue()).getTime()));

                put.add(STATUS, Bytes.toBytes("source"), Bytes.toBytes(json.path("source").getTextValue()));
                put.add(STATUS, Bytes.toBytes("text"), Bytes.toBytes(json.path("text").getTextValue()));
                put.add(STATUS, Bytes.toBytes("truncated"), Bytes.toBytes(json.path("truncated").getBooleanValue()));

                if(json.path("in_reply_to_status_id").getLongValue() > 0L) {
                    put.add(STATUS, Bytes.toBytes("in_reply_to_status_id"), Bytes.toBytes(json.path("in_reply_to_status_id").getLongValue()));
                }
                if(json.path("in_reply_to_user_id").getLongValue() > 0L) {
                    put.add(STATUS, Bytes.toBytes("in_reply_to_user_id"), Bytes.toBytes(json.path("in_reply_to_user_id").getLongValue()));
                }
                if(json.path("in_reply_to_screen_name").getTextValue() != null) {
                    put.add(STATUS, Bytes.toBytes("in_reply_to_screen_name"), Bytes.toBytes(json.path("in_reply_to_screen_name").getTextValue()));
                }

                put.add(STATUS, Bytes.toBytes("favorited"), Bytes.toBytes(json.path("favorited").getBooleanValue()));
                put.add(STATUS, Bytes.toBytes("retweeted"), Bytes.toBytes(json.path("retweeted").getBooleanValue()));
                put.add(STATUS, Bytes.toBytes("retweet_count"), Bytes.toBytes(json.path("retweet_count").getIntValue()));

                JsonNode place = json.path("place");
                if(!place.isNull()) {
                    put.add(PLACE, Bytes.toBytes("id"), Bytes.toBytes(place.path("id").getTextValue()));
                    put.add(PLACE, Bytes.toBytes("name"), Bytes.toBytes(place.path("name").getTextValue()));
                    put.add(PLACE, Bytes.toBytes("full_name"), Bytes.toBytes(place.path("full_name").getTextValue()));
                    put.add(PLACE, Bytes.toBytes("url"), Bytes.toBytes(place.path("url").getTextValue()));
                    put.add(PLACE, Bytes.toBytes("place_type"), Bytes.toBytes(place.path("place_type").getTextValue()));
                    put.add(PLACE, Bytes.toBytes("country"), Bytes.toBytes(place.path("country").getTextValue()));
                    put.add(PLACE, Bytes.toBytes("country_code"), Bytes.toBytes(place.path("country_code").getTextValue()));
                    if(!place.path("bounding_box").isNull()) {
                        put.add(PLACE, Bytes.toBytes("bounding_box"), Bytes.toBytes(place.path("bounding_box").path("coordinates").toString()));
                    }
                    if(!place.path("bounding_box").isNull()) {
                        put.add(PLACE, Bytes.toBytes("bounding_box_type"), Bytes.toBytes(place.path("bounding_box").path("type").getTextValue()));
                    }
                    put.add(PLACE, Bytes.toBytes("attributes"), Bytes.toBytes(place.path("attributes").toString()));
                }

                for(JsonNode userMention : json.path("entities").path("user_mentions")) {
                    byte[] userMentionId = Bytes.toBytes(userMention.path("id").getLongValue());
                    put.add(USER_MENTIONS, Bytes.add(userMentionId, Bytes.toBytes("id")), userMentionId);
                    put.add(USER_MENTIONS, Bytes.add(userMentionId, Bytes.toBytes("name")), Bytes.toBytes(userMention.path("name").getTextValue()));
                    put.add(USER_MENTIONS, Bytes.add(userMentionId, Bytes.toBytes("screen_name")), Bytes
                            .toBytes(userMention.path("screen_name").getTextValue()));
                    put.add(USER_MENTIONS, Bytes.add(userMentionId, Bytes.toBytes("indices")), Bytes.toBytes(userMention.path("indices").toString()));
                }

                for(JsonNode url : json.path("entities").path("urls")) {
                    byte[] urls = Bytes.toBytes(url.path("url").getTextValue());
                    put.add(URLS, Bytes.add(urls, Bytes.toBytes("url")), urls);
                    if(!url.path("expanded_url").isNull()) {
                        put.add(URLS, Bytes.add(urls, Bytes.toBytes("expanded_url")), Bytes.toBytes(url.path("expanded_url").getTextValue()));
                    }
                    put.add(URLS, Bytes.add(urls, Bytes.toBytes("indices")), Bytes.toBytes(url.path("indices").toString()));
                }

                for(JsonNode hashtag : json.path("entities").path("hashtags")) {
                    put.add(HASHTAGS, Bytes.toBytes(hashtag.path("text").getTextValue()), Bytes.toBytes(hashtag.path("indices").toString()));
                }

                JsonNode user = json.path("user");
                put.add(USER, Bytes.toBytes("id"), Bytes.toBytes(user.path("id").getLongValue()));
                put.add(USER, Bytes.toBytes("name"), Bytes.toBytes(user.path("name").getTextValue()));
                put.add(USER, Bytes.toBytes("screen_name"), Bytes.toBytes(user.path("screen_name").getTextValue()));
                put.add(USER, Bytes.toBytes("created_at"), Bytes.toBytes(createdAtDateFormat().parse(user.path("created_at").getTextValue()).getTime()));
                if(!user.path("description").isNull()) {
                    put.add(USER, Bytes.toBytes("description"), Bytes.toBytes(user.path("description").getTextValue()));
                }
                if(!user.path("url").isNull()) {
                    put.add(USER, Bytes.toBytes("url"), Bytes.toBytes(user.path("url").getTextValue()));
                }

                put.add(USER, Bytes.toBytes("lang"), Bytes.toBytes(user.path("lang").getTextValue()));
                if(!user.path("location").isNull()) {
                    put.add(USER, Bytes.toBytes("location"), Bytes.toBytes(user.path("location").getTextValue()));
                }
                if(!user.path("time_zone").isNull()) {
                    put.add(USER, Bytes.toBytes("time_zone"), Bytes.toBytes(user.path("time_zone").getTextValue()));
                }
                if(user.path("utc_offset").isNumber()) {
                    put.add(USER, Bytes.toBytes("utc_offset"), Bytes.toBytes(user.path("utc_offset").getIntValue()));
                }

                put.add(USER, Bytes.toBytes("statuses_count"), Bytes.toBytes(user.path("statuses_count").getIntValue()));
                put.add(USER, Bytes.toBytes("favourites_count"), Bytes.toBytes(user.path("favourites_count").getIntValue()));
                put.add(USER, Bytes.toBytes("followers_count"), Bytes.toBytes(user.path("followers_count").getIntValue()));
                put.add(USER, Bytes.toBytes("friends_count"), Bytes.toBytes(user.path("friends_count").getIntValue()));
                put.add(USER, Bytes.toBytes("listed_count"), Bytes.toBytes(user.path("listed_count").getIntValue()));

                put.add(USER, Bytes.toBytes("profile_image_url"), Bytes.toBytes(user.path("profile_image_url").getTextValue()));
                put.add(USER, Bytes.toBytes("profile_background_image_url"), Bytes.toBytes(user.path("profile_background_image_url").getTextValue()));
                if(!user.path("profile_text_color").isNull()) {
                    put.add(USER, Bytes.toBytes("profile_text_color"), Bytes.toBytes(user.path("profile_text_color").getTextValue()));
                }
                if(!user.path("profile_link_color").isNull()) {
                    put.add(USER, Bytes.toBytes("profile_link_color"), Bytes.toBytes(user.path("profile_link_color").getTextValue()));
                }
                if(!user.path("profile_sidebar_fill_color").isNull()) {
                    put.add(USER, Bytes.toBytes("profile_sidebar_fill_color"), Bytes.toBytes(user.path("profile_sidebar_fill_color").getTextValue()));
                }
                if(!user.path("profile_sidebar_border_color").isNull()) {
                    put.add(USER, Bytes.toBytes("profile_sidebar_border_color"), Bytes.toBytes(user.path("profile_sidebar_border_color").getTextValue()));
                }
                if(!user.path("profile_background_color").isNull()) {
                    put.add(USER, Bytes.toBytes("profile_background_color"), Bytes.toBytes(user.path("profile_background_color").getTextValue()));
                }
                put.add(USER, Bytes.toBytes("profile_background_tile"), Bytes.toBytes(user.path("profile_background_tile").getBooleanValue()));
                put.add(USER, Bytes.toBytes("profile_use_background_image"), Bytes.toBytes(user.path("profile_use_background_image").getBooleanValue()));

                put.add(USER, Bytes.toBytes("protected"), Bytes.toBytes(user.path("protected").getBooleanValue()));
                put.add(USER, Bytes.toBytes("following"), Bytes.toBytes(user.path("following").getBooleanValue()));
                put.add(USER, Bytes.toBytes("follow_request_sent"), Bytes.toBytes(user.path("follow_request_sent").getBooleanValue()));

                put.add(USER, Bytes.toBytes("notifications"), Bytes.toBytes(user.path("notifications").getBooleanValue()));
                put.add(USER, Bytes.toBytes("verified"), Bytes.toBytes(user.path("verified").getBooleanValue()));
                put.add(USER, Bytes.toBytes("geo_enabled"), Bytes.toBytes(user.path("geo_enabled").getBooleanValue()));
                put.add(USER, Bytes.toBytes("contributors_enabled"), Bytes.toBytes(user.path("contributors_enabled").getBooleanValue()));
                put.add(USER, Bytes.toBytes("show_all_inline_media"), Bytes.toBytes(user.path("show_all_inline_media").getBooleanValue()));
                put.add(USER, Bytes.toBytes("is_translator"), Bytes.toBytes(user.path("is_translator").getBooleanValue()));
            }
            catch(ParseException e) {
                LOG.warn(e.getMessage(), e);
            }
        }
        else {
            id = json.path("delete").path("status").path("id").getLongValue();
            if(id > 0L) {
                // delete
                put = new Put(makeKey(event.getTimestamp(), id), event.getTimestamp());

                byte[] cfDelete = Bytes.toBytes("delete");
                put.add(cfDelete, Bytes.toBytes("id"), Bytes.toBytes(id));
                put.add(cfDelete, Bytes.toBytes("user_id"), Bytes.toBytes(json.path("delete").path("status").path("user_id").getLongValue()));
            }
        }

        if(table != null) {
            if(put != null) {
                table.put(put);
            }
            else {
                LOG.warn(json.toString());
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.cloudera.flume.core.EventSink.Base#close()
     */
    @Override
    public void close() throws IOException, InterruptedException {
        if(table != null) {
            try {
                table.close();
            }
            finally {
                table = null;
            }
        }
    }

    /**
     * @param ts
     * @param id
     * @return
     */
    private byte[] makeKey(long ts, long id) {
        return Bytes.add(new byte[] { (byte) (ts & 0x0f) }, Bytes.toBytes(Long.MAX_VALUE - ts), Bytes.toBytes(id));
    }

    /**
     * @return
     */
    private SimpleDateFormat createdAtDateFormat() {
        return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
    }

}
