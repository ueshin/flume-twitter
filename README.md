# flume-twitter

This is a flume plugin for twitter streaming.

## How to build

Maven2 or later is needed.

Download or `git clone` this project.

Build it as follows:

    $ mvn clean package

You will find a packaged jar file in `target` directory.

## How to install

Put the jar file into ${FLUME_HOME}/lib directory or add to `FLUME_CLASSPATH`.

Add the following configuration to your flume-site.xml:

    <configuration>
      <property>
        <name>flume.plugin.classes</name>
        <value>st.happy_camper.flume.twitter.TwitterStreamingPlugin</value>
      </property>
    </configuration>

Restart flume processes then you will see "Twitter" source in "extn" page of Flume Master web ui.

## How to configure

Configure "Twitter" source as follows:

    Twitter("<your twitter id>", "<password>")

and any sink.

You will find Twitter Streaming data in your sink.

## License

Apache License, Version 2.0


## Author

Takuya UESHIN (ueshin@happy-camper.st) 
