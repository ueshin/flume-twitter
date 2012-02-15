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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * @author ueshin
 */
public class TwitterStreamingPlugin {

    private TwitterStreamingPlugin() {
    }

    /**
     * @return
     */
    public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
        List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
        builders.add(new Pair<String, SourceBuilder>("Twitter", new SourceBuilder() {

            @Override
            public EventSource build(Context ctx, String... args) {
                Preconditions.checkArgument(args.length <= 3, "usage: Twitter[(name[, password[, connectionTimeout]])]");

                FlumeConfiguration conf = FlumeConfiguration.get();

                String name = conf.getTwitterName();
                if(args.length > 0) {
                    name = args[0];
                }
                String password = conf.getTwitterPW();
                if(args.length > 1) {
                    password = args[1];
                }
                int connectionTimeout = 1000; // ms
                if(args.length > 2) {
                    connectionTimeout = Integer.parseInt(args[1]);
                }
                return new TwitterStreamingSource(name, password, connectionTimeout);
            }
        }));
        return builders;
    }

    /**
     * @return
     */
    public static List<Pair<String, SinkBuilder>> getSinkBuilders() {
        List<Pair<String, SinkBuilder>> builders = new ArrayList<Pair<String, SinkBuilder>>();
        builders.add(new Pair<String, SinkBuilder>("TwitterToHBase", new SinkBuilder() {

            @Override
            public EventSink build(Context context, String... args) {
                Preconditions.checkArgument(args.length <= 1, "usage: TwitterToHBase[(tableName)]");

                FlumeConfiguration conf = FlumeConfiguration.get();
                String tableName = conf.get(TwitterStreamingHBaseSink.class.getName() + ".tableName", "timeline");
                if(args.length > 0) {
                    tableName = args[0];
                }
                return new TwitterStreamingHBaseSink(HBaseConfiguration.create(conf), tableName);
            }
        }));
        return builders;
    }

}
