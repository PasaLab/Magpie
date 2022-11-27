import search.GeneticSearch;
import util.InitLog;
import util.Config;

import org.slf4j.Logger;

import java.util.*;

public class ParameterOptimization {
    private static final Logger LOG = InitLog.initLog(ParameterOptimization.class);

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
        GeneticSearch geneticSearch = new GeneticSearch(paras);

        System.out.println("==========参数搜索开始==========");
        geneticSearch.iterateSearch(target);
     }

}
