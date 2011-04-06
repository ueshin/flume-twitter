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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;

/**
 * @author ueshin
 */
public class Status {

    public static final byte[] FAMILY = Bytes.toBytes("status");

    public static final byte[] ID = Bytes.toBytes("id");

    public static final byte[] CREATED_AT = Bytes.toBytes("created_at");

    public static final byte[] SOURCE = Bytes.toBytes("source");

    public static final byte[] TEXT = Bytes.toBytes("text");

    public static final byte[] TRUNCATED = Bytes.toBytes("truncated");

    public static final byte[] IN_REPLY_TO_STATUS_ID = Bytes.toBytes("in_reply_to_status_id");

    public static final byte[] IN_REPLY_TO_USER_ID = Bytes.toBytes("in_reply_to_user_id");

    public static final byte[] IN_REPLY_TO_SCREEN_NAME = Bytes.toBytes("in_reply_to_screen_name");

    public static final byte[] FAVORITED = Bytes.toBytes("favorited");

    public static final byte[] RETWEETED = Bytes.toBytes("retweeted");

    public static final byte[] RETWEET_COUNT = Bytes.toBytes("retweet_count");

    public final long id;

    public final Date createdAt;

    public final String source;

    public final String text;

    public final boolean truncated;

    public final Long inReplyToStatusId;

    public final Long inReplyToUserId;

    public final String inReplyToScreenName;

    public final boolean favorited;

    public final boolean retweeted;

    public final int retweetCount;

    public final Place place;

    public final List<UserMention> userMentions;

    public final List<Url> urls;

    public final List<Hashtag> hashtags;

    public final User user;

    /**
     * @throws ParseException
     */
    public Status(JsonNode json) throws Exception {
        id = json.path("id").getLongValue();
        createdAt = createdAtDateFormat().parse(json.path("created_at").getTextValue());

        source = json.path("source").getTextValue();
        text = json.path("text").getTextValue();
        truncated = json.path("truncated").getBooleanValue();

        inReplyToStatusId = json.path("in_reply_to_status_id").isNull() ? null : json.path("in_reply_to_status_id").getLongValue();
        inReplyToUserId = json.path("in_reply_to_user_id").isNull() ? null : json.path("in_reply_to_user_id").getLongValue();
        inReplyToScreenName = json.path("in_reply_to_screen_name").isNull() ? null : json.path("in_reply_to_screen_name").getTextValue();

        favorited = json.path("favorited").getBooleanValue();
        retweeted = json.path("retweeted").getBooleanValue();
        retweetCount = json.path("retweet_count").getIntValue();

        place = json.path("place").isNull() ? null : new Place(json.path("place"));

        List<UserMention> userMentions = new ArrayList<UserMention>();
        for(JsonNode userMention : json.path("entities").path("user_mentions")) {
            userMentions.add(new UserMention(userMention));
        }
        this.userMentions = Collections.unmodifiableList(userMentions);

        List<Url> urls = new ArrayList<Url>();
        for(JsonNode url : json.path("entities").path("urls")) {
            urls.add(new Url(url));
        }
        this.urls = Collections.unmodifiableList(urls);

        List<Hashtag> hashtags = new ArrayList<Hashtag>();
        for(JsonNode hashtag : json.path("entities").path("hashtags")) {
            hashtags.add(new Hashtag(hashtag));
        }
        this.hashtags = Collections.unmodifiableList(hashtags);

        this.user = new User(json.path("user"));
    }

    /**
     * @return
     */
    private SimpleDateFormat createdAtDateFormat() {
        return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
    }

}
