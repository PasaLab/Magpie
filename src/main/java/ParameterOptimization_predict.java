import org.slf4j.Logger;
import search.GeneticSearch;
import search.GeneticSearch_predict;
import util.Config;
import util.InitLog;

import java.util.Arrays;
import java.util.Map;

public class ParameterOptimization_predict {
    private static final Logger LOG = InitLog.initLog(ParameterOptimization_predict.class);

    public static void main(String args[]){
        //获取所有参数和取值
        Map<String, String[]> paras = Config.getParameters();
        System.out.println("搜索参数集合：");
        for(String k : paras.keySet()){
            System.out.println(k + "："+Arrays.toString(paras.get(k)).replace(" ",""));
        }
        //获取目标性能
        Double target = Double.parseDouble(Config.getString("target"));
        System.out.println("性能目标为：" + target);

        System.out.println("==========参数预选开始==========");
        GeneticSearch_predict geneticSearch = new GeneticSearch_predict(paras);

        System.out.println("==========参数搜索开始==========");
        geneticSearch.iterateSearch(target);
     }

}
