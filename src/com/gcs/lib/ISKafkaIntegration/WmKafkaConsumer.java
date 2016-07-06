package com.gcs.lib.ISKafkaIntegration;

import com.softwareag.util.IDataMap;
import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.Session;
import com.wm.data.IData;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
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


    static java.util.concurrent.ConcurrentMap<String,Boolean> closedMap = new java.util.concurrent.ConcurrentHashMap<>() ;
    static java.util.concurrent.ConcurrentMap<String,KafkaConsumer> nameConsumerMap = new java.util.concurrent.ConcurrentHashMap<>() ;



    public void createConsumer(String topicName  , String bootstrapServers , String clientGroupId , String enableAutoCommit , Integer autoCommitIntervalInMs ,
                               Integer sessionTimeOut , String serviceName , String name

    ){

         if(enableAutoCommit == null || enableAutoCommit.equals(""))
             enableAutoCommit = "false" ;

         if(autoCommitIntervalInMs == null)
             autoCommitIntervalInMs = 1000 ;

        if(sessionTimeOut == null)
            sessionTimeOut = 30000 ;

        Properties props = new Properties();
        System.out.println("configure property");
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", clientGroupId);
        props.put("enable.auto.commit", enableAutoCommit);
        props.put("auto.commit.interval.ms", autoCommitIntervalInMs);
        props.put("session.timeout.ms", sessionTimeOut);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.gcs.lib.ISKafkaIntegration.IDataDeserializer");
        System.out.println("property configured");

        Session session = Service.getSession();

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        System.out.println("subscription enabled");

        ExecutorService executor = Executors.newFixedThreadPool(1) ;
        ExecutorService servExecutor = Executors.newFixedThreadPool(3) ;

        closedMap.put(name,false) ;
        nameConsumerMap.put(name, consumer) ;

        String finalEnableAutoCommit = enableAutoCommit;
        FutureTask<Void>  invokeTask  =  new FutureTask<Void>(() -> {

        while(!closedMap.get(name)){


               ConsumerRecords<String,IData> records = consumer.poll(100);

               for(ConsumerRecord<String,IData> record : records){
                   System.out.println("retrieved record :: service invoke about to happen");

                   invokeService( record ,serviceName , session);

                   System.out.println("service invoked");
               }

           if(finalEnableAutoCommit.equalsIgnoreCase("false")) consumer.commitSync();

        }

            return null ;
       });

        executor.execute(invokeTask);

        executor.shutdown();
    }

    private void invokeService(ConsumerRecord<String, IData> record, String serviceName, Session session) {
        IData input =  IDataFactory.create() ;

        IDataMap inputmap = new IDataMap(input) ;

        inputmap.put( "data" , record.value()) ;

        new InvokeState();

        InvokeState.setCurrentSession(session);
        InvokeState.setCurrentUser(session.getUser());


        try {
            Service.doInvoke( NSName.create(serviceName) , IDataUtil.deepClone(input));
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


    public static void main(String[] args) {
        WmKafkaConsumer cs = new WmKafkaConsumer();
         cs.createConsumer("wmtopic","localhost:9092,localhost:9093,localhost:9094","cg","false",null,null,"abc","mysubs");
    }


}


