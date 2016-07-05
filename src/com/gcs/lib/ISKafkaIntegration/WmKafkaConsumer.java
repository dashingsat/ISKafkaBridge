package com.gcs.lib.ISKafkaIntegration;

import com.wm.app.b2b.server.Session;
import com.wm.data.IData;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by sdas on 5/31/2016.
 */
public class WmKafkaConsumer {

    public interface Action{

        public void invoke(String serviceName , ConsumerRecords<String, IData> record , Session session ) ;
    }

    public void createConsumer(String topicName  , Action action , String bootstrapServers , String clientGroupId , String enableAutoCommit , String autoCommitIntervalInMs ,
                               String sessionTimeOut

    ){
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", clientGroupId);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalInMs);
        props.put("session.timeout.ms", sessionTimeOut);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.gcs.lib.ISKafkaIntegration.IDataSerializer");

        ExecutorService executor = Executors.newFixedThreadPool(1) ;


       FutureTask<Boolean>  invokeTask  =  new FutureTask<Boolean>(() -> {



           return false ;
       });
    }

}


