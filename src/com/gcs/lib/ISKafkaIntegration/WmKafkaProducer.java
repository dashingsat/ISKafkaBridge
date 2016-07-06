package com.gcs.lib.ISKafkaIntegration;

import com.softwareag.util.IDataMap;
import com.wm.app.b2b.server.InvokeState;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.Session;
import com.wm.data.IData;
import com.wm.data.IDataFactory;
import com.wm.lang.ns.NSName;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by sdas on 5/31/2016.
 */
public class WmKafkaProducer {

    private Producer producer ;

    private String bootstrapServers ;
    private String acks ;
    private Integer retries ;
    private Integer batchSize ;
    private Integer lingerInMs ;
    private Integer bufferMem ;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getAcks() {
        return acks;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public Integer getRetries() {
        return retries;
    }

    public Integer getLingerInMs() {
        return lingerInMs;
    }

    public Integer getBufferMem() {
        return bufferMem;
    }

    public WmKafkaProducer(String bootstrapServers, Integer retries, String acks, Integer batchSize, Integer lingerInMs, Integer bufferMem) {
        this.bootstrapServers = bootstrapServers;

        if(retries == null )
            this.retries = 0 ;
        else
         this.retries = retries ;

        if(acks == null || acks.equals(""))
            this.acks = "all" ;

        else
          this.acks = acks ;

        if(retries == null)
            retries = 0 ;
        else
        this.retries = retries ;

        if(batchSize == null)
            this.batchSize = 16384 ;
        else
        this.batchSize = batchSize ;

        if(bufferMem == null)
            this.bufferMem = 33554432 ;
        else
         this.bufferMem = bufferMem ;

        if(lingerInMs == null)
            this.lingerInMs = 1 ;
        else
            this.lingerInMs = lingerInMs ;

       createProducer() ;



    }

    public Boolean createProducer () {

        try{
            System.out.println("creating producer config");

            Properties props = new Properties();

            props.put("bootstrap.servers" ,this.getBootstrapServers() ) ;

            props.put("retries" , this.getRetries() ) ;

            props.put("acks" , this.getAcks()) ;

            props.put("batch.size" , this.getBatchSize());

            props.put("buffer.memory" , this.getBufferMem());

            props.put("linger.ms" ,this.getLingerInMs()) ;
            System.out.println("got props");

            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") ;

            props.put("value.serializer", "com.gcs.lib.ISKafkaIntegration.IDataSerializer") ;

            System.out.println("creating producer");

            producer = new KafkaProducer<String , IData>(props) ;

            System.out.println("producer created");

            return true ;


        }
        catch (Exception e){

            System.out.println(e.toString());
        }

        return false ;
    }

     public Boolean send(String topicName , String keyName , IData value , String callbackService ){

         Session session = Service.getSession() ;


         if(callbackService != null && !callbackService.equals(""))
         {
             System.out.println("in the first block");
             try{
                 producer.send(new ProducerRecord<String,IData>(topicName , keyName , value) , (recordMetadata, e) -> {
                     if(e != null)
                         e.printStackTrace();
                     else{

                         invokeService(recordMetadata , session , callbackService) ;

                     }

                 });
             }catch(Exception e){
                 System.out.println(e.toString());
                 return false ;
             }


             return true ;

         }

         else{
             try{
                 System.out.println("in the second block");
                 producer.send(new ProducerRecord<>(topicName, keyName, value)) ;
                 System.out.println("sent");
                 return true ;
             }catch(Exception e){
                 System.out.println(e.toString());
                 return false ;
             }

         }



     }

    private void invokeService(RecordMetadata recordMetadata, Session session , String callbackService) {

        IData input =  IDataFactory.create() ;

        IDataMap inputmap = new IDataMap(input) ;

        inputmap.put( "topic" , recordMetadata.topic()) ;

        inputmap.put("partition"  , recordMetadata.partition()) ;

        inputmap.put("offset" , recordMetadata.offset()) ;

        new InvokeState();

        InvokeState.setCurrentSession(session);
        InvokeState.setCurrentUser(session.getUser());


        try {
              Service.doInvoke( NSName.create(callbackService) , input);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void close(){
        producer.close();
    }

    public static void main(String[] args) {
        String bs = "localhost:9092,localhost:9093,localhost:9094" ;
        String acks = "all" ;
        WmKafkaProducer prod = new WmKafkaProducer(bs ,0 ,"all" , null , null , null);
    }

}
