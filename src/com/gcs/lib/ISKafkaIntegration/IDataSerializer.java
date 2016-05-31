package com.gcs.lib.ISKafkaIntegration;

import com.wm.data.IData;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * Created by Satyabrata Dash on 5/31/2016.
 */
public class IDataSerializer implements Serializer<IData> {

    private ByteArrayOutputStream bos = new ByteArrayOutputStream();
    private ObjectOutput out = null;
    private byte[] yourBytes = null ;

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, IData data) {


        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(data);
            yourBytes = bos.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }

        }

        return yourBytes ;
    }

    @Override
    public void close() {
        try {
            if (out != null) {
                out.close();
            }
        } catch (IOException ex) {
            // ignore close exception
        }
        try {
            bos.close();
        } catch (IOException ex) {
            // ignore close exception
        }
    }
}
