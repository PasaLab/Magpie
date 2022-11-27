package prediction;

import util.Config;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class LGBMPredict {
    private static final Logger LOG = LoggerFactory.getLogger(LGBMPredict.class);
    private static String jobType = Config.getString("flink.job.type");
    String baseDir = System.getProperties().getProperty("user.dir");


    //特征提取,合并并返回特征向量
    public String getFeature(){
        JobFeature job = new JobFeature();
        DataSetFeature dataSet = new DataSetFeature();
        List<Double> feature = new ArrayList<>();
        String inputFeature = null;
        //可能存在多作业
        for(List<Double> j: job.getFeature(jobType)) {
            feature.addAll(j);
            feature.addAll(dataSet.getFeature(jobType));

            LOG.info("合并特征："+feature);
            inputFeature = StringUtils.strip(feature.toString(),"[]").replace(" ", "");
            writeToTrainData(inputFeature);
        }
        return inputFeature;
    }


    private void writeToTrainData(String content){

        String fileName = baseDir+ String.format("/src/main/resources/trainCache/%s", jobType);
        //fileName = "D:\\test.txt";
        LOG.info("特征集路径:" + fileName);
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


    public String getLightGBM(){
        String inputFeature = getFeature();
        String res = null;
        try{
            String[] pythonData =new String[]{Config.getString("python.cmd"), baseDir+"/src/main/java/prediction/LightGBM.py",
                    jobType, inputFeature};
            LOG.info("参数预测执行命令"+ Arrays.toString(pythonData));
            //读取到python文件
            Process pr = Runtime.getRuntime().exec(pythonData);
            InputStreamReader ir = new InputStreamReader(pr.getInputStream());
            LineNumberReader in = new LineNumberReader(ir);
            String line ;
            // 打印LightGBM模型的预测结果
            while((line=in.readLine()) != null){
                res = line;
                LOG.info("参数预选结果：" + line);
            }
            ir.close();
            in.close();
            pr.waitFor();
            pr.destroy();
        }catch(Exception e){
            e.printStackTrace();
        }
        return res;
    }

    public Map<String, String> getParameter(){
        String[] parasValue = getLightGBM().split(",");
        Map<String, String> paras = new LinkedHashMap<>();
        paras.put("taskmanager.memory.process.size", parasValue[0]);
        paras.put("taskmanager.numberOfTaskSlots", parasValue[1]);
        paras.put("taskmanager.memory.network.fraction", parasValue[2]);
        paras.put("taskmanager.memory.managed.fraction", parasValue[3]);
        paras.put("parallelism.default",parasValue[4]);
        return paras;
    }




    public static void main(String[] args) throws IOException {
       LGBMPredict lgbmPredict = new LGBMPredict();
        System.out.println(lgbmPredict.getFeature());

    }



}
