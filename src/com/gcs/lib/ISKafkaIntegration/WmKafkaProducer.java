package com.gcs.lib.ISKafkaIntegration;

import com.wm.data.IData;
import com.wm.util.template.IfVarElseToken;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

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
            batchSize = 16384 ;
        else
        this.batchSize = batchSize ;

        if(bufferMem == null)
            this.bufferMem = 33554432 ;
        else
         this.bufferMem = bufferMem ;

        if(lingerInMs == null)
            lingerInMs = 0 ;




    }

    public Boolean createProducer () {

        try{

            Properties props = new Properties();

            props.put("bootstrap.servers" ,this.getBootstrapServers() ) ;

            props.put("retries" , this.getRetries() ) ;

            props.put("acks" , this.getAcks()) ;

            props.put("batch.size" , this.getBatchSize());

            props.put("buffer.memory" , this.getBufferMem());

            props.put("linger.ms" ,this.getLingerInMs()) ;

            producer = new KafkaProducer<String , IData>(props) ;



        }
        catch (Exception e){

            System.out.println(e.toString());
        }

        return false ;
    }


}
