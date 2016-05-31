package com.gcs.lib.ISKafkaIntegration;

import com.wm.data.IData;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Created by sdas on 5/31/2016.
 */
public class IDataDesrializer implements Deserializer<IData>{

    ByteArrayInputStream bis = null ;
    ObjectInput in = null;
    IData data = null ;
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public IData deserialize(String topic, byte[] bytes) {

        bis =  new ByteArrayInputStream(bytes);
        try {
            in = new ObjectInputStream(bis);
            data = (IData) in.readObject();

        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }

        return data ;
    }

    @Override
    public void close() {
        try {
            bis.close();
        } catch (IOException ex) {
            // ignore close exception
        }
        try {
            if (in != null) {
                in.close();
            }
        } catch (IOException ex) {
            // ignore close exception
        }
    }
}
