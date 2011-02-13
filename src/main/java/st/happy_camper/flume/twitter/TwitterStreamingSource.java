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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Pair;
import com.google.common.base.Preconditions;

/**
 * @author ueshin
 */
public class TwitterStreamingSource extends EventSource.Base {

    private final String name;

    private final String password;

    /**
     * @param name
     * @param password
     */
    public TwitterStreamingSource(String name, String password) {
        this.name = name;
        this.password = password;
    }

    private TwitterStreamingConnection conn;

    /*
     * (non-Javadoc)
     * @see com.cloudera.flume.core.EventSource.Base#open()
     */
    @Override
    public synchronized void open() throws IOException {
        if(conn == null) {
            conn = new TwitterStreamingConnection(name, password);
        }
        else {
            throw new IllegalStateException();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.cloudera.flume.core.EventSource.Base#next()
     */
    @Override
    public Event next() throws IOException {
        if(conn != null) {
            String status = conn.take();
            if(status != null) {
                Event event = new EventImpl(status.getBytes("utf8"));
                event.set(Event.A_SERVICE, "Twitter".getBytes("utf8"));
                updateEventProcessingStats(event);
                return event;
            }
            else {
                return null;
            }
        }
        else {
            throw new IllegalStateException();
        }
    }

    /*
     * (non-Javadoc)
     * @see com.cloudera.flume.core.EventSource.Base#close()
     */
    @Override
    public synchronized void close() throws IOException {
        if(conn != null) {
            conn.close();
            conn = null;
        }
    }

    /**
     * @return
     */
    public static List<Pair<String, SourceBuilder>> getSourceBuilders() {
        List<Pair<String, SourceBuilder>> builders = new ArrayList<Pair<String, SourceBuilder>>();
        builders.add(new Pair<String, SourceBuilder>("Twitter", new SourceBuilder() {

            @Override
            public EventSource build(Context ctx, String... args) {
                Preconditions.checkArgument(args.length <= 2, "usage: Twitter[(name[, password])]");

                FlumeConfiguration conf = FlumeConfiguration.get();

                String name = conf.getTwitterName();
                if(args.length > 0) {
                    name = args[0];
                }
                String password = conf.getTwitterPW();
                if(args.length > 1) {
                    password = args[1];
                }
                return new TwitterStreamingSource(name, password);
            }
        }));
        return builders;
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String... args) throws IOException {
        if(args.length != 2) {
            System.err.println("Usage: TwitterStreamingSource name password");
            System.exit(-1);
        }

        String name = args[0];
        String password = args[1];

        final TwitterStreamingSource src = new TwitterStreamingSource(name, password);

        src.open();

        new Thread() {

            public void run() {
                try {
                    while(true) {
                        System.out.println(src.next());
                    }
                }
                catch(IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

}
