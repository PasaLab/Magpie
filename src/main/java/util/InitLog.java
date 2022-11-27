package util;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class InitLog {
    public static Logger initLog(Class name){
        String baseDir = System.getProperty("AutoConf.dir");
        if (baseDir == null) {
            // 开发环境中使用
            baseDir = System.getProperty("user.dir");
            System.setProperty("AutoConf.dir", baseDir);
        }
        FileInputStream fileInputStream = null;
        try {
            Properties properties = new Properties();
            fileInputStream = new FileInputStream(baseDir+"/conf/log4j.properties");
            properties.load(fileInputStream);
            PropertyConfigurator.configure(properties);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return LoggerFactory.getLogger(name);
    }

}
