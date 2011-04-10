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
import java.io.IOException;
import java.nio.channels.IllegalSelectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import st.happy_camper.flume.twitter.entity.Delete;
import st.happy_camper.flume.twitter.entity.Hashtag;
import st.happy_camper.flume.twitter.entity.Place;
import st.happy_camper.flume.twitter.entity.Status;
import st.happy_camper.flume.twitter.entity.Url;
import st.happy_camper.flume.twitter.entity.User;
import st.happy_camper.flume.twitter.entity.UserMention;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;

/**
 * @author ueshin
 */
public class TwitterStreamingHBaseSink extends EventSink.Base {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStreamingHBaseSink.class);

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
        Put put;
        try {
            JsonNode json = new ObjectMapper().readTree(new ByteArrayInputStream(event.getBody()));

            if(json.path("delete").isMissingNode()) {
                Status status = new Status(json);

                put = new Put(Bytes.add(Bytes.toBytes(status.user.id), Bytes.toBytes(Long.MAX_VALUE - status.id)), event.getTimestamp());

                put.add(Status.FAMILY, Status.ID, Bytes.toBytes(status.id));
                put.add(Status.FAMILY, Status.CREATED_AT, Bytes.toBytes(status.createdAt.getTime()));

                put.add(Status.FAMILY, Status.SOURCE, Bytes.toBytes(status.source));
                put.add(Status.FAMILY, Status.TEXT, Bytes.toBytes(status.text));
                put.add(Status.FAMILY, Status.TRUNCATED, Bytes.toBytes(status.truncated));

                if(status.inReplyToStatusId != null) {
                    put.add(Status.FAMILY, Status.IN_REPLY_TO_STATUS_ID, Bytes.toBytes(status.inReplyToStatusId));
                }
                if(status.inReplyToUserId != null) {
                    put.add(Status.FAMILY, Status.IN_REPLY_TO_USER_ID, Bytes.toBytes(status.inReplyToUserId));
                }
                if(status.inReplyToScreenName != null) {
                    put.add(Status.FAMILY, Status.IN_REPLY_TO_SCREEN_NAME, Bytes.toBytes(status.inReplyToScreenName));
                }

                put.add(Status.FAMILY, Status.FAVORITED, Bytes.toBytes(status.favorited));
                put.add(Status.FAMILY, Status.RETWEETED, Bytes.toBytes(status.retweeted));
                put.add(Status.FAMILY, Status.RETWEET_COUNT, Bytes.toBytes(status.retweetCount));

                Place place = status.place;
                if(place != null) {
                    put.add(Place.FAMILY, Place.ID, Bytes.toBytes(place.id));
                    put.add(Place.FAMILY, Place.NAME, Bytes.toBytes(place.name));
                    put.add(Place.FAMILY, Place.FULL_NAME, Bytes.toBytes(place.fullName));
                    put.add(Place.FAMILY, Place.URL, Bytes.toBytes(place.url));
                    put.add(Place.FAMILY, Place.PLACE_TYPE, Bytes.toBytes(place.placeType));
                    put.add(Place.FAMILY, Place.COUNTRY, Bytes.toBytes(place.country));
                    put.add(Place.FAMILY, Place.COUNTRY_CODE, Bytes.toBytes(place.countryCode));
                    if(place.boundingBox != null) {
                        put.add(Place.FAMILY, Place.BOUNDING_BOX, Bytes.toBytes(place.boundingBox));
                    }
                    if(place.boundingBoxType != null) {
                        put.add(Place.FAMILY, Place.BOUNDING_BOX_TYPE, Bytes.toBytes(place.boundingBoxType));
                    }
                    put.add(Place.FAMILY, Place.ATTRIBUTES, Bytes.toBytes(place.attributes));
                }

                for(UserMention userMention : status.userMentions) {
                    byte[] id = Bytes.toBytes(userMention.id);
                    put.add(UserMention.FAMILY, Bytes.add(id, UserMention.ID), id);
                    put.add(UserMention.FAMILY, Bytes.add(id, UserMention.NAME), Bytes.toBytes(userMention.name));
                    put.add(UserMention.FAMILY, Bytes.add(id, UserMention.SCREEN_NAME), Bytes.toBytes(userMention.screenName));
                    put.add(UserMention.FAMILY, Bytes.add(id, UserMention.INDICES), Bytes.toBytes(userMention.indices));
                }

                for(Url url : status.urls) {
                    byte[] id = Bytes.toBytes(url.url);
                    put.add(Url.FAMILY, Bytes.add(id, Url.URL), id);
                    if(url.expandedUrl != null) {
                        put.add(Url.FAMILY, Bytes.add(id, Url.EXPANDED_URL), Bytes.toBytes(url.expandedUrl));
                    }
                    put.add(Url.FAMILY, Bytes.add(id, Url.INDICES), Bytes.toBytes(url.indices));
                }

                for(Hashtag hashtag : status.hashtags) {
                    put.add(Hashtag.FAMILY, Bytes.toBytes(hashtag.text), Bytes.toBytes(hashtag.indices));
                }

                User user = status.user;
                put.add(User.FAMILY, User.ID, Bytes.toBytes(user.id));
                put.add(User.FAMILY, User.NAME, Bytes.toBytes(user.name));
                put.add(User.FAMILY, User.SCREEN_NAME, Bytes.toBytes(user.screenName));
                put.add(User.FAMILY, User.CREATED_AT, Bytes.toBytes(user.createdAt.getTime()));
                if(user.description != null) {
                    put.add(User.FAMILY, User.DESCRIPTION, Bytes.toBytes(user.description));
                }
                if(user.url != null) {
                    put.add(User.FAMILY, User.URL, Bytes.toBytes(user.url));
                }

                put.add(User.FAMILY, User.LANG, Bytes.toBytes(user.lang));
                if(user.location != null) {
                    put.add(User.FAMILY, User.LOCATION, Bytes.toBytes(user.location));
                }
                if(user.timeZone != null) {
                    put.add(User.FAMILY, User.TIME_ZONE, Bytes.toBytes(user.timeZone));
                }
                if(user.utcOffset != null) {
                    put.add(User.FAMILY, User.UTC_OFFSET, Bytes.toBytes(user.utcOffset));
                }

                put.add(User.FAMILY, User.STATUSES_COUNT, Bytes.toBytes(user.statusesCount));
                put.add(User.FAMILY, User.FAVOURITES_COUNT, Bytes.toBytes(user.favouritesCount));
                put.add(User.FAMILY, User.FOLLOWERS_COUNT, Bytes.toBytes(user.followersCount));
                put.add(User.FAMILY, User.FRIENDS_COUNT, Bytes.toBytes(user.friendsCount));
                put.add(User.FAMILY, User.LISTED_COUNT, Bytes.toBytes(user.listedCount));

                put.add(User.FAMILY, User.PROFILE_IMAGE_URL, Bytes.toBytes(user.profileImageUrl));
                put.add(User.FAMILY, User.PROFILE_BACKGROUND_IMAGE_URL, Bytes.toBytes(user.profileBackgroundImageUrl));
                if(user.profileTextColor != null) {
                    put.add(User.FAMILY, User.PROFILE_TEXT_COLOR, Bytes.toBytes(user.profileTextColor));
                }
                if(user.profileLinkColor != null) {
                    put.add(User.FAMILY, User.PROFILE_LINK_COLOR, Bytes.toBytes(user.profileLinkColor));
                }
                if(user.profileSidebarFillColor != null) {
                    put.add(User.FAMILY, User.PROFILE_SIDEBAR_FILL_COLOR, Bytes.toBytes(user.profileSidebarFillColor));
                }
                if(user.profileSidebarBorderColor != null) {
                    put.add(User.FAMILY, User.PROFILE_SIDEBAR_BORDER_COLOR, Bytes.toBytes(user.profileSidebarBorderColor));
                }
                if(user.profileBackgroundColor != null) {
                    put.add(User.FAMILY, User.PROFILE_BACKGROUND_COLOR, Bytes.toBytes(user.profileBackgroundColor));
                }
                put.add(User.FAMILY, User.PROFILE_BACKGROUND_TILE, Bytes.toBytes(user.profileBackgroundTile));
                put.add(User.FAMILY, User.PROFILE_USE_BACKGROUND_IMAGE, Bytes.toBytes(user.profileUseBackgroundImage));

                put.add(User.FAMILY, User.PROTECTED, Bytes.toBytes(user.isProtected));
                put.add(User.FAMILY, User.FOLLOWING, Bytes.toBytes(user.following));
                put.add(User.FAMILY, User.FOLLOW_REQUEST_SENT, Bytes.toBytes(user.followRequestSent));

                put.add(User.FAMILY, User.NOTIFICATIONS, Bytes.toBytes(user.notifications));
                put.add(User.FAMILY, User.VERIFIED, Bytes.toBytes(user.verified));
                put.add(User.FAMILY, User.GEO_ENABLED, Bytes.toBytes(user.geoEnabled));
                put.add(User.FAMILY, User.CONTRIBUTORS_ENABLED, Bytes.toBytes(user.contributorsEnabled));
                put.add(User.FAMILY, User.SHOW_ALL_INLINE_MEDIA, Bytes.toBytes(user.showAllInlineMedia));
                put.add(User.FAMILY, User.IS_TRANSLATOR, Bytes.toBytes(user.isTranslator));
            }
            else {
                Delete delete = new Delete(json);

                put = new Put(Bytes.add(Bytes.toBytes(delete.userId), Bytes.toBytes(Long.MAX_VALUE - delete.id)), event.getTimestamp());
                put.add(Delete.FAMILY, Delete.ID, Bytes.toBytes(delete.id));
                put.add(Delete.FAMILY, Delete.USER_ID, Bytes.toBytes(delete.userId));
            }
        }
        catch(Exception e) {
            LOG.warn(e.getMessage(), e);
            return;
        }

        if(table != null) {
            table.put(put);
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

}
