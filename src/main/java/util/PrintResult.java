package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public class PrintResult {
    String baseDir = System.getProperties().getProperty("user.dir");
    private static String jobType = Config.getString("flink.job.type");

    public void printMetrics(Map<String, String> result){
        for(String key : result.keySet()){
            if(key.contains("NonHeap")){
                System.out.println("JVM堆外内存利用率："+result.get(key));
            }
            if(key.contains("_Heap")){
                System.out.println("JVM堆内存利用率："+result.get(key));
            }
            if(key.contains("time"))
                System.out.println("作业执行时间："+result.get(key));
            if(key.contains("CPU"))
                System.out.println("CPU负载："+ result.get(key));
            if(key.contains("Network"))
                System.out.println("网络缓存利用率："+result.get(key));
        }
    }

    public void writeTrainData(Map<String, String> result){
        StringBuilder sb = new StringBuilder();
        for(String k : result.keySet()){
            sb.append(","+result.get(k));
        }
        String content = sb.toString()+"\n";
        String fileName = baseDir+ String.format("/src/main/resources/train/%s", jobType);
        //fileName = "D:\\test.txt";
        Path path = Paths.get(fileName);
        File file= new File(fileName);
        // 使用newBufferedWriter创建文件并写文件
        // 这里使用了try-with-resources方法来关闭流，不用手动关闭
        if(!file.exists()) {
            try (BufferedWriter writer =
                         Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
                writer.write(content);
            }catch (IOException e){
                e.printStackTrace();
            }
        }else {
            //追加写模式
            try (BufferedWriter writer =
                         Files.newBufferedWriter(path,
                                 StandardCharsets.UTF_8,
                                 StandardOpenOption.APPEND)) {
                writer.write(content);
            }catch (IOException e){
                e.printStackTrace();
            }
        }

    }
}
