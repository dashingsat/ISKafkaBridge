package com.gcs.lib.ISKafkaIntegration;

import com.softwareag.util.IDataMap;
import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.Session;
import com.wm.data.IData;
import com.wm.data.IDataFactory;
import com.wm.data.IDataTableCursor;
import com.wm.lang.ns.NSName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by sdas on 5/31/2016.
 */
public class WmKafkaConsumer {

    public KafkaConsumer<String,IData> consumer ;

    private AtomicBoolean closed  ;
    static java.util.concurrent.ConcurrentMap<String,Boolean> closedMap ;
    static java.util.concurrent.ConcurrentMap<String,KafkaConsumer> nameConsumerMap ;

    public interface Action{

        public void invoke(String serviceName , ConsumerRecord<String, IData> record , Session session ) ;
    }

    public void createConsumer(String topicName  , Action action , String bootstrapServers , String clientGroupId , String enableAutoCommit , String autoCommitIntervalInMs ,
                               String sessionTimeOut , String serviceName , String name

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
        ExecutorService servExecutor = Executors.newFixedThreadPool(3) ;


       closed.set(false) ;

        closedMap.put(name,false) ;
        nameConsumerMap.put(name, consumer) ;

       FutureTask<Void>  invokeTask  =  new FutureTask<Void>(() -> {


        while(closedMap.get(name)){


               ConsumerRecords<String,IData> records = consumer.poll(100);

               for(ConsumerRecord<String,IData> record : records){

                   invokeService( record ,serviceName , session);


               }

           if(enableAutoCommit.equalsIgnoreCase("false")) consumer.commitSync();

        }

            return null ;
       });

        executor.execute(invokeTask);
    }

    private void invokeService(ConsumerRecord<String, IData> record, String serviceName, Session session) {
        IData input =  IDataFactory.create() ;

        IDataMap inputmap = new IDataMap(input) ;

        inputmap.put( "data" , record.value()) ;

        new InvokeState();

        InvokeState.setCurrentSession(session);
        InvokeState.setCurrentUser(session.getUser());


        try {
            Service.doInvoke( NSName.create(serviceName) , input);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static Boolean close (String name){

        try{

            closedMap.put(name , true);
            nameConsumerMap.get(name).close();
            return true ;
        }catch(Exception e ){
            return false ;
        }


    }



}


