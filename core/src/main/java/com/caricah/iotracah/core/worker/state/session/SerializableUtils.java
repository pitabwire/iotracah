package com.caricah.iotracah.core.worker.state.session;

import org.apache.shiro.codec.Base64;
import org.apache.shiro.session.Session;
import org.nustaq.serialization.FSTConfiguration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Just picked as is from github:
 *
 *      https://github.com/zhangkaitao/shiro-example/blob/master/shiro-example-chapter10/src/main/java/com/github/zhangkaitao/shiro/chapter10/SerializableUtils.java
 *
 * <p>User: Zhang Kaitao
 * <p>Date: 14-2-8
 * <p>Version: 1.0
 */
public final class SerializableUtils {

    private static FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    public static String serialize(Session session) {
        try {
            //            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            //            ObjectOutputStream oos = new ObjectOutputStream(bos);
            //            oos.writeObject(session);
            //            return Base64.encodeToString(bos.toByteArray());
            //

            return Base64.encodeToString(conf.asByteArray(session));
        } catch (Exception e) {
            throw new RuntimeException("serialize session error", e);
        }
    }
    public static Session deserialize(String sessionStr) {
        try {
            //            ByteArrayInputStream bis = new ByteArrayInputStream(Base64.decode(sessionStr));
            //            ObjectInputStream ois = new ObjectInputStream(bis);
            //            return (Session)ois.readObject();
            //

            return (Session) conf.asObject(Base64.decode(sessionStr));
        } catch (Exception e) {
            throw new RuntimeException("deserialize session error", e);
        }
    }
}
