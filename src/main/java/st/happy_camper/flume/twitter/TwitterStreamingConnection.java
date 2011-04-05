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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ueshin
 */
public class TwitterStreamingConnection {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterStreamingConnection.class);

    private static final String URL = "http://stream.twitter.com/1/statuses/sample.json";

    private final Random rnd = new Random();

    private final HttpClient httpClient;

    private HttpMethod method = null;

    private BufferedReader reader = null;

    private BlockingQueue<String> queue = new LinkedBlockingQueue<String>();

    /**
     * @param name
     * @param password
     * @throws IOException
     */
    public TwitterStreamingConnection(String name, String password) throws IOException {
        httpClient = new HttpClient();
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(1000);
        httpClient.getHttpConnectionManager().getParams().setSoTimeout(10 * 1000);
        httpClient.getParams().setAuthenticationPreemptive(true);
        httpClient.getState().setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(name, password));

        doOpen();

        Executors.newSingleThreadExecutor(new ThreadFactory() {

            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, "TwitterStreamingConnection");
            }
        }).execute(new Runnable() {

            @Override
            public void run() {
                BlockingQueue<String> queue = TwitterStreamingConnection.this.queue;

                String line;
                while((line = readLine()) != null) {
                    queue.add(line);
                }
            }
        });
    }

    /**
     * @return
     * @throws InterruptedException
     */
    public String take() {
        try {
            return (queue != null) ? queue.take() : null;
        }
        catch(InterruptedException e) {
            return null;
        }
    }

    /**
     * 
     */
    public synchronized void close() {
        queue = null;
        method.releaseConnection();
    }

    /**
     * @return
     */
    private String readLine() {
        try {
            String line = reader.readLine();
            if(line != null) {
                return line;
            }
            else {
                synchronized(this) {
                    if(queue != null) {
                        method.releaseConnection();

                        doOpen();
                        return readLine();
                    }
                    else {
                        return null;
                    }
                }
            }
        }
        catch(IOException e) {
            synchronized(this) {
                if(queue != null) {
                    LOG.warn(e.getMessage(), e);

                    Thread t = new Thread() {

                        @Override
                        public void run() {
                            try {
                                while(reader.readLine() != null)
                                    ;
                            }
                            catch(IOException e) {
                            }
                        }

                    };
                    t.start();

                    method.releaseConnection();

                    try {
                        t.join();
                    }
                    catch(InterruptedException ie) {
                    }

                    doOpen();
                    return readLine();
                }
                else {
                    return null;
                }
            }
        }
    }

    /**
     * @return
     */
    private void doOpen() {
        int backoff = 10000;

        while(true) {
            HttpMethod method = new GetMethod(URL);
            try {
                int statusCode = httpClient.executeMethod(method);
                switch(statusCode) {
                    case 200: {
                        LOG.info("Connected.");
                        this.method = method;
                        this.reader = new BufferedReader(new InputStreamReader(method.getResponseBodyAsStream(), "utf8"));
                        return;
                    }
                    case 401:
                    case 403:
                    case 406:
                    case 413:
                    case 416: {
                        String message = String.format("%d %s.", statusCode, HttpStatus.getStatusText(statusCode));
                        LOG.warn(message);
                        method.releaseConnection();

                        throw new IllegalStateException(message);
                    }
                    case 420: {
                        LOG.warn("420 Rate Limited.");
                        method.releaseConnection();
                        break;
                    }
                    case 500: {
                        LOG.warn("500 Server Internal Error.");
                        method.releaseConnection();
                        break;
                    }
                    case 503: {
                        LOG.warn("503 Service Overloaded.");
                        method.releaseConnection();
                        break;
                    }
                    default: {
                        String message = "Unknown status code: " + statusCode;
                        LOG.warn(message);
                        method.releaseConnection();
                    }
                }
            }
            catch(IllegalStateException e) {
                throw e;
            }
            catch(Exception e) {
                LOG.error(e.getMessage(), e);
                method.releaseConnection();
            }

            int wait = rnd.nextInt(backoff) + 1;
            LOG.info(String.format("Retry after %d milliseconds.", wait));
            try {
                Thread.sleep(wait);
            }
            catch(InterruptedException e) {
                LOG.error(e.getMessage(), e);
            }
            if(backoff < 240000) {
                backoff *= 2;
                if(backoff >= 240000) {
                    backoff = 240000;
                }
            }
        }
    }

}
