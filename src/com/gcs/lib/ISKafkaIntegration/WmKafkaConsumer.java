package com.gcs.lib.ISKafkaIntegration;

import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.Session;
import com.wm.data.IData;
import com.wm.data.IDataTableCursor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by sdas on 5/31/2016.
 */
public class WmKafkaConsumer {

    public KafkaConsumer<String,IData> consumer ;

    public interface Action{

        public void invoke(String serviceName , ConsumerRecord<String, IData> record , Session session ) ;
    }

    public void createConsumer(String topicName  , Action action , String bootstrapServers , String clientGroupId , String enableAutoCommit , String autoCommitIntervalInMs ,
                               String sessionTimeOut , String serviceName

    ){

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", clientGroupId);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalInMs);
        props.put("session.timeout.ms", sessionTimeOut);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.gcs.lib.ISKafkaIntegration.IDataSerializer");

        Session session = Service.getSession();

        consumer = new KafkaConsumer<String,IData>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        ExecutorService executor = Executors.newFixedThreadPool(1) ;


       FutureTask<Void>  invokeTask  =  new FutureTask<>(() -> {

        while(true){
               ConsumerRecords<String,IData> records = consumer.poll(100);

               for(ConsumerRecord<String,IData> record : records){

                  // invokeService(serviceName , ses)
               }


        }

       });
    }

}


