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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;

/**
 * @author ueshin
 */
public class User {

    public static final byte[] FAMILY = Bytes.toBytes("user");

    public static final byte[] ID = Bytes.toBytes("id");

    public static final byte[] NAME = Bytes.toBytes("name");

    public static final byte[] SCREEN_NAME = Bytes.toBytes("screen_name");

    public static final byte[] CREATED_AT = Bytes.toBytes("created_at");

    public static final byte[] DESCRIPTION = Bytes.toBytes("description");

    public static final byte[] URL = Bytes.toBytes("url");

    public static final byte[] LANG = Bytes.toBytes("lang");

    public static final byte[] LOCATION = Bytes.toBytes("location");

    public static final byte[] TIME_ZONE = Bytes.toBytes("time_zone");

    public static final byte[] UTC_OFFSET = Bytes.toBytes("utc_offset");

    public static final byte[] STATUSES_COUNT = Bytes.toBytes("statuses_count");

    public static final byte[] FAVOURITES_COUNT = Bytes.toBytes("favourites_count");

    public static final byte[] FOLLOWERS_COUNT = Bytes.toBytes("followers_count");

    public static final byte[] FRIENDS_COUNT = Bytes.toBytes("friends_count");

    public static final byte[] LISTED_COUNT = Bytes.toBytes("listed_count");

    public static final byte[] PROFILE_IMAGE_URL = Bytes.toBytes("profile_image_url");

    public static final byte[] PROFILE_BACKGROUND_IMAGE_URL = Bytes.toBytes("profile_background_image_url");

    public static final byte[] PROFILE_TEXT_COLOR = Bytes.toBytes("profile_text_color");

    public static final byte[] PROFILE_LINK_COLOR = Bytes.toBytes("profile_link_color");

    public static final byte[] PROFILE_SIDEBAR_FILL_COLOR = Bytes.toBytes("profile_sidebar_fill_color");

    public static final byte[] PROFILE_SIDEBAR_BORDER_COLOR = Bytes.toBytes("profile_sidebar_border_color");

    public static final byte[] PROFILE_BACKGROUND_COLOR = Bytes.toBytes("profile_background_color");

    public static final byte[] PROFILE_BACKGROUND_TILE = Bytes.toBytes("profile_background_tile");

    public static final byte[] PROFILE_USE_BACKGROUND_IMAGE = Bytes.toBytes("profile_use_background_image");

    public static final byte[] PROTECTED = Bytes.toBytes("protected");

    public static final byte[] FOLLOWING = Bytes.toBytes("following");

    public static final byte[] FOLLOW_REQUEST_SENT = Bytes.toBytes("follow_request_sent");

    public static final byte[] NOTIFICATIONS = Bytes.toBytes("notifications");

    public static final byte[] VERIFIED = Bytes.toBytes("verified");

    public static final byte[] GEO_ENABLED = Bytes.toBytes("geo_enabled");

    public static final byte[] CONTRIBUTORS_ENABLED = Bytes.toBytes("contributors_enabled");

    public static final byte[] SHOW_ALL_INLINE_MEDIA = Bytes.toBytes("show_all_inline_media");

    public static final byte[] IS_TRANSLATOR = Bytes.toBytes("is_translator");

    public final long id;

    public final String name;

    public final String screenName;

    public final Date createdAt;

    public final String description;

    public final String url;

    public final String lang;

    public final String location;

    public final String timeZone;

    public final Integer utcOffset;

    public final int statusesCount;

    public final int favouritesCount;

    public final int followersCount;

    public final int friendsCount;

    public final int listedCount;

    public final String profileImageUrl;

    public final String profileBackgroundImageUrl;

    public final String profileTextColor;

    public final String profileLinkColor;

    public final String profileSidebarFillColor;

    public final String profileSidebarBorderColor;

    public final String profileBackgroundColor;

    public final boolean profileBackgroundTile;

    public final boolean profileUseBackgroundImage;

    public final boolean isProtected;

    public final boolean following;

    public final boolean followRequestSent;

    public final boolean notifications;

    public final boolean verified;

    public final boolean geoEnabled;

    public final boolean contributorsEnabled;

    public final boolean showAllInlineMedia;

    public final boolean isTranslator;

    /**
     * @param path
     * @throws Exception
     */
    public User(JsonNode json) throws Exception {
        id = json.path("id").getLongValue();
        name = json.path("name").getTextValue();
        screenName = json.path("screen_name").getTextValue();
        createdAt = createdAtDateFormat().parse(json.path("created_at").getTextValue());
        description = json.path("description").getTextValue();
        url = json.path("url").getTextValue();

        lang = json.path("lang").getTextValue();
        location = json.path("location").getTextValue();
        timeZone = json.path("time_zone").getTextValue();
        utcOffset = json.path("utc_offset").isNumber() ? json.path("utc_offset").getIntValue() : null;

        statusesCount = json.path("statuses_count").getIntValue();
        favouritesCount = json.path("favourites_count").getIntValue();
        followersCount = json.path("followers_count").getIntValue();
        friendsCount = json.path("friends_count").getIntValue();
        listedCount = json.path("listed_count").getIntValue();

        profileImageUrl = json.path("profile_image_url").getTextValue();
        profileBackgroundImageUrl = json.path("profile_background_image_url").getTextValue();
        profileTextColor = json.path("profile_text_color").getTextValue();
        profileLinkColor = json.path("profile_link_color").getTextValue();
        profileSidebarFillColor = json.path("profile_sidebar_fill_color").getTextValue();
        profileSidebarBorderColor = json.path("profile_sidebar_border_color").getTextValue();
        profileBackgroundColor = json.path("profile_background_color").getTextValue();
        profileBackgroundTile = json.path("profile_background_tile").getBooleanValue();
        profileUseBackgroundImage = json.path("profile_use_background_image").getBooleanValue();

        isProtected = json.path("protected").getBooleanValue();
        following = json.path("following").getBooleanValue();
        followRequestSent = json.path("follow_request_sent").getBooleanValue();

        notifications = json.path("notifications").getBooleanValue();
        verified = json.path("verified").getBooleanValue();
        geoEnabled = json.path("geo_enabled").getBooleanValue();
        contributorsEnabled = json.path("contributors_enabled").getBooleanValue();
        showAllInlineMedia = json.path("show_all_inline_media").getBooleanValue();
        isTranslator = json.path("is_translator").getBooleanValue();
    }

    /**
     * @return
     */
    private SimpleDateFormat createdAtDateFormat() {
        return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
    }

}
