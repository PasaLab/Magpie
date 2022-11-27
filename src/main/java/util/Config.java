package util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Config {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    /**
     *  除去参数配置单位
     * @param config
     * @return
     */
    public static int getInt(String config){
        return Integer.parseInt(getString(config).replaceAll("[^0-9]", ""));
    }

    public static double getDouble(String config){
        Pattern p = Pattern.compile("(\\d+(\\.\\d+)?)");
        Matcher matcher = p.matcher(getString(config));
        while (matcher.find()){
            return Double.parseDouble(matcher.group());
        }
       // return Double.parseDouble(matcher.replaceAll(""));
        return 1d;
    }

    /**
     * object类型解析
     * @param clz
     * @param o
     * @param <T>
     * @return
     */
    public static  <T> T get(Class<T> clz,Object o){
        if(clz.isInstance(o)){
            return clz.cast(o);
        }
        return null;
    }

    /**
     * 取config对应的string
     * @param config
     * @return
     */
    public  static String getString(String config){
        Map<String, Object> map = getConfig();
        return map.get(config).toString();
    }

    /**
     * 取config里对应的map
     * @param config
     * @return
     */
    public  static Map<String, Object> getMap(String config){
        Map<String, Object> map = getConfig();
        Object res = map.get(config);
        if(res instanceof Map){
            return (Map<String, Object>)res;
        }
        return null;
    }

//后面准备参数变成取值范围
//    public static  String[] disperse(String start, String end){
//        String unit =  start.replaceAll("[0-9]", "");
//        List<Integer> range = IntStream.rangeClosed(getInt(start), getInt(end))
//                .boxed().collect(Collectors.toList());
//    }

    /**
     * 获取参数及其取值范围
     * @return
     */
    public static Map<String, String[]> getParameters(){
        Map<String, Object> map =getMap("parameters");
        Map<String, String[]> paras = new LinkedHashMap<>();
        for(String k : map.keySet()){
            String res = map.get(k).toString();
            String[] values = res.substring(1, res.length()-1).split(", ");
            //后面准备修改
//            if(values.length == 2 && Character.isDigit(values[1].charAt(0)))
//                values = disperse(values[0], values[0]);
            paras.put(k, values);
        }
       // LOG.debug("weijiajia");
        return paras;
    }

    /**
     * 获取指标及其权重
     * @return
     */
    public static Map<String, Double> getMetrics(){
        Map<String,Object> objectMap =  Config.getMap("metrics");
        Map<String, Double> metrics = new LinkedHashMap<>();
        for(String key: objectMap.keySet()){
            metrics.put(key, get(Double.class, objectMap.get(key)));
        }
        return metrics;
    }

    /**
     * 获取项目配置
     * @return
     */
    public static Map<String, Object> getConfig( ) {
        LinkedHashMap<String, Object> yamls = new LinkedHashMap<>();
        Yaml yaml = new Yaml();
        String confDir = System.getProperty("AutoConf.conf.dir");
        if (confDir == null) {
            File file = new File(Config.class.getClassLoader().getResource("config.yaml").getPath());
            confDir = file.getParent();
        }

        try {
            InputStream in = new FileInputStream(confDir + "/config.yaml");
            //本地测试
            //InputStream in = Config.class.getClassLoader().getResourceAsStream("config.yaml");
            yamls = yaml.loadAs(in, LinkedHashMap.class);
        } catch (Exception e) {
            LOG.error("{} load failed !!!", "config.yaml");
        }
        return yamls;
    }

    public static Map<String, Object> getYaml( String path ) {
        LinkedHashMap<String, Object> yamls = new LinkedHashMap<>();
        Yaml yaml = new Yaml();
        try {
            InputStream in = new FileInputStream(path);
            yamls = yaml.loadAs(in, LinkedHashMap.class);
        } catch (Exception e) {
            LOG.error("{} load failed !!!", "config.yaml");
        }
        return yamls;
    }

    private final static DumperOptions OPTIONS = new DumperOptions();

    static {
        //设置yaml读取方式为块读取
        OPTIONS.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        OPTIONS.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
        OPTIONS.setPrettyFlow(false);
    }


    public static List<String> getFlinkConf(String fileName){
        List<String> config = new ArrayList<>();
        try (FileReader reader = new FileReader(fileName);
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            while ((line = br.readLine()) != null) {
                // 一次读入一行数据
                config.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return config;
    }

    /**
     * 将新的参数配置写入flink-conf.yaml文件
     * @return
     */
    public static boolean modifyFlinkConf(Map<String, String> paras) {
        String yamlDir = getString("flink.dir" ) + "/conf/flink-conf.yaml";
        List<String> list = getFlinkConf(yamlDir);
        if (null == list || null == paras) {
            LOG.error("flink-conf.yaml文件为空~~~");
            return false;
        }
        int flag = 0;
        try {
            File writeName = new File(yamlDir); // 相对路径，如果没有则要建立一个新的output.txt文件
            writeName.createNewFile(); // 创建新文件,有同名的文件的话直接覆盖
            try (FileWriter writer = new FileWriter(writeName);
                 BufferedWriter out = new BufferedWriter(writer)
            ) {
                for (String line : list) {
                    if (!line.contains("#")) {
                        for (String key : paras.keySet()) {
                            String value = paras.get(key);
                            if(line.contains(key) && !value.equals(line.split(": ")[1])){
                                line = String.format("%s: %s", key, value);
                                flag = 1;
                                LOG.info(String.format("修改参数：%s, 新参数值：%s", key, value));
                              //  paras.remove(key);
                                break;
                            }
                         }
                    }
                    out.write(line + "\n");
                }
                out.flush(); // 把缓存区内容压入文件
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (flag == 0) {
                LOG.warn("参数没有修改过~~~");
            }
            return true;
        }
    }
}