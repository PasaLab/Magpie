package search;

import util.Config;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class ParameterRule {
    private static final Logger LOG = LoggerFactory.getLogger(ParameterRule.class);

    private static final double networkMemoryMin = Config.getDouble("taskmanager.memory.network.min");
    private static final double networkMemoryMax = Config.getDouble("taskmanager.memory.network.max");
    private static final double slotMemoryMin = Config.getDouble("memory.slot.min");
    private static final double slotMemoryMax = Config.getDouble("memory.slot.max");

    /**
     * 过滤参数单位 取数值
     * @param str
     * @return
     */
    public static Double getDouble(String str){

        Pattern r = Pattern.compile("([1-9]\\d*\\.?\\d*)|(0\\.\\d*[1-9])");
        Matcher matcher = r.matcher(str);
        if(matcher.find()){
            return Double.parseDouble(matcher.group(0));
        }
        return 0d;
    }

    /**
     * 筛选规则1 并行度/slot个数<=集群节点数
     * @param env
     * @return
     */
    public static boolean rule1(Map<String, Object> env){

        String expression = Config.getString("rule1");;
        Expression rightExp = AviatorEvaluator.compile(rightExp(expression));
        Boolean res = (Boolean)rightExp.execute(env);;
        if (res){
            LOG.info("参数配置符合规则1");
        }else
            LOG.info("参数配置不符合规则1");
        return  res;
    }


    /**
     * 筛选规则2 min<TM总内存/slot个数<max
     * @param env
     * @return
     */
    public static boolean rule2(Map<String, Object> env){
        String expression = Config.getString("rule2");
        Expression leftExp = AviatorEvaluator.compile(leftExp(expression));
        Expression rightExp = AviatorEvaluator.compile(rightExp(expression));
        Boolean res = (Boolean) leftExp.execute(env) && (Boolean)rightExp.execute(env);
        if (res){
            LOG.info("参数配置符合规则2");
        }else {
            LOG.info("参数配置不符合规则2");
        }
        return  res;
    }

    /**
     * 筛选规则3 min<network比例*TM总内存<max
     * @param env
     * @return
     */
    public static boolean rule3(Map<String, Object> env){
        String expression = Config.getString("rule3");
        Expression leftExp = AviatorEvaluator.compile(leftExp(expression));
        Expression rightExp = AviatorEvaluator.compile(rightExp(expression));
        Boolean res = (Boolean) leftExp.execute(env) && (Boolean)rightExp.execute(env);
        if (res){
            LOG.info("参数配置符合规则3");
        }else
            LOG.info("参数配置不符合规则3");
        return  res;
    }

    //参数约束规则（表达式）解析
    public static String leftExp(String exp){
       String[] exps = exp.split("<=");
       if(exps.length < 2) {
           LOG.error("约束规则表达式解析错误：" + exps);
           return null;
       }
       if(exps[0].contains("g") || exps[0].contains("G"))
           return getDouble(exps[0])*1024 + "<=" +exps[1].replace(".","");
       return getDouble(exps[0]) + "<=" +exps[1].replace(".","");
    }

    public static String rightExp(String exp){
        String[] exps = exp.split("<=");
        if(exps.length < 2) {
            LOG.error("约束规则表达式解析错误：" + exps);
            return null;
        }
        if(exps[exps.length-1].contains("g") || exps[exps.length-1].contains("G"))
            return exps[exps.length-2].replace(".","") + "<=" + getDouble(exps[exps.length-1])*1024 ;
        return exps[exps.length-2].replace(".","") + "<=" + getDouble(exps[exps.length-1]);
    }


    public static boolean rule (Map<String, String> config) {
        Map<String, Object> env = new HashMap<>();
        for(String k : config.keySet()){
            if(k.equals("taskmanager.memory.process.size"))
                env.put(k.replace(".",""), getDouble(config.get(k))*1024);
            else
                env.put(k.replace(".",""), getDouble(config.get(k)));
        }
        return rule1(env) && rule2(env) && rule3(env);
    }

//    public static void main(String[] args) {
//        LinkedHashMap<String, String> initialConfig = new LinkedHashMap<>();
//        initialConfig.put("taskmanager.memory.process.size", "10g");
//        initialConfig.put("taskmanager.numberOfTaskSlots", "4");
//        initialConfig.put("taskmanager.memory.managed.fraction", "0.4");
//        initialConfig.put("taskmanager.memory.network.fraction", "0.1");
//        initialConfig.put("parallelism.default", "1");
//        System.out.println(rule(initialConfig));
//
//    }

}
