package prediction;

import util.Config;
import util.HdfsClient;
import common.HiveParse;
import common.QueryUtil;
import graph.Graph;
import common.GraphConverter;
import org.apache.commons.cli.*;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
1 sql数据集特征处理
2 其他数据集基础特征
 */
public class DataSetFeature {
    private static final Logger LOG = LoggerFactory.getLogger(DataSetFeature.class);
    private static String cmd = Config.getString("job.submit.cmd");


    private static final Option HIVE_CONF = new Option("c", "hive_conf", true,
            "conf of hive.");

    private static final Option DATABASE = new Option("d", "database", true,
            "database of hive.");

    private static final Option LOCATION = new Option("l", "location", true,
            "sql query path.");

    private static final Option QUERIES = new Option("q", "queries", true,
            "sql query names. If the value is 'all', all queries will be executed.");

    private static final Option ITERATIONS = new Option("i", "iterations", true,
            "The number of iterations that will be run per case, default is 1.");

    private static final Option POINTS = new Option("p", "points", true,
            "KMeans points");

    private static final Option CENTROIDS = new Option("c", "centroids", true,
            "KMeans centroids");

    private static final Option TRAIN = new Option("tr", "train", true,
            "KNN train");

    private static final Option TEST = new Option("te", "test", true,
            "KNN test");

    private static final Option K= new Option("k", "k", true,
            "KNN k");

    private static final Option INPUT = new Option("in", "input", true,
            "WordCount input");

    private static final Option VERTICES = new Option("v", "vertices", true,
            "ConnectedComponents vertices");

    private static final Option EDGES = new Option("e", "edges", true,
            "ConnectedComponents edges");

    private static final Option PAGES = new Option("pa", "pages", true,
            "PageRank pages");

    private static final Option LINKS = new Option("li", "links", true,
            "PageRank links");
    private static final Option OUTPUT = new Option("out", "output", true,
            "computed result");
    private static final Option NUMPAGES = new Option("num", "numpages", true,
            "number of page");

    /**
     * 根据输入命令的参数 获取相应数据集
     * @return
     */

    private static Options getOptions() {
        Options options = new Options();
        //SQL
        options.addOption(HIVE_CONF);
        options.addOption(DATABASE);
        options.addOption(LOCATION);
        options.addOption(QUERIES);
        options.addOption(ITERATIONS);
        //KMeans
        options.addOption(POINTS);
        options.addOption(CENTROIDS);
        //KNN
        options.addOption(TRAIN);
        options.addOption(TEST);
        options.addOption(K);
        //wordcount
        options.addOption(INPUT);
        //连通分量
        options.addOption(VERTICES);
        options.addOption(EDGES);
        //PageRank
        options.addOption(PAGES);
        options.addOption(LINKS);
        options.addOption(OUTPUT);
        options.addOption(NUMPAGES);

        return options;
    }

    /**
     * 获取TPC-DS数据集的所有表相关联列的特征
     * @return
     */

    public static HashMap<String, Map<String, Double>> getTableFeature(){
        HiveParse hiveParse  = new HiveParse();
        HashMap<String, Map<String, Double>> res = new HashMap<>();
        try {
            Options options = getOptions();
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(options,cmd.split(""), true);

            //暂时不用，后面看看
            //LinkedHashMap<String, String> queries = QueryUtil.getQueries(line.getOptionValue(LOCATION.getOpt()), line.getOptionValue(QUERIES.getOpt()));
            LinkedHashMap<String, String> queries = QueryUtil.getQueries(line.getOptionValue(LOCATION.getOpt()), null);
            Map<String, Set<String>> result = new LinkedHashMap<>();
            for(String sql : queries.keySet()){
                //针对一条sql，分析关键key的重复率
                HashMap<String, Set<String>> tabCol = hiveParse.getKeyColumn(queries.get(sql));
                Map<String, Double> temp = new HashMap<>();
                for(String tab : tabCol.keySet()){
                    Set<String> col = result.get(tab);
                    if(col == null){
                        col = new HashSet<>();
                    }
                    col.addAll(tabCol.get(tab));
                    result.put(tab, col);
                  //  temp.putAll(QueryUtil.getRepetition(tab, tabCol.get(tab),line.getOptionValue(DATABASE.getOpt())));
                }
                res.put(sql, temp);
            }

            for(String tab : result.keySet()){
                System.out.println(tab + ":" + result.get(tab));
            }
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("sql解析失败或计算重复率");
        }
        return res;
    }

    public static String[]getArgs(String cmd){
        String args = cmd.substring(cmd.indexOf(".jar")+5);
        LOG.info("提交命令参数："+args);
        return args.split(" |  ");
    }

    /**
     * 按照固定的特征名进行 特征值排序
     * @param features
     * @return
     */
    public static List<Double> sortFeature(Map<String, Double> features, Double size){

        List<String> tableName = Arrays.asList("item","web_sales","date_dim","store_sales","catalog_sales",
                "customer_demographics","call_center","catalog_returns","customer","customer_address","store","store_returns");
        List<Double> featureValue =  new ArrayList<>();
        featureValue.add(size);
        for (String name : tableName){
           if(features.containsKey(name+"_avg")) {
               featureValue.add(features.get(name + "_avg"));
               featureValue.add(features.get(name + "_var"));
           }else {
               featureValue.add(0d);
               featureValue.add(0d);
           }

        }
        return featureValue;
    }

    public static List<Double> getSQLFeature(){
        HiveParse hiveParse  = new HiveParse();
        Map<String, Double> res = new LinkedHashMap<>();
        Double size = 0d;
        try {
            Options options = getOptions();
            CommandLineParser parser = new DefaultParser( );
            CommandLine line = parser.parse(options,getArgs(cmd));
            //获取当前执行sql
            LinkedHashMap<String, String> queries = QueryUtil.getQueries(line.getOptionValue(LOCATION.getOpt()), line.getOptionValue(QUERIES.getOpt()));
            //获取数据集大小
            size = getDouble(line.getOptionValue(DATABASE.getOpt()));
            //获取tpc-ds 全部sql
            //LinkedHashMap<String, String> queries = QueryUtil.getQueries(line.getOptionValue(LOCATION.getOpt()), null);
            if(queries.size() == 0){
                LOG.error("查询结果为空");
                return null;
            }
            Map<String, Set<String>> tabCol = new LinkedHashMap<>();
            //解析SQL作业，获取表的计算列
            for(String sql : queries.keySet()){
                Map<String, Set<String>> temp = hiveParse.getKeyColumn(queries.get(sql));
                for(String tab : temp.keySet()){
                    Set<String> col = tabCol.get(tab);
                    if(col == null)
                        col = new HashSet<>();
                    col.addAll(temp.get(tab));
                    tabCol.put(tab, col);
                }

            }
            //hive 计算列的平均基数和方差
            for(String tab : tabCol.keySet()){
                LOG.info(String.format("计算表%s的平均基数和平均基数方差",tab));
                if(tab.equals(" ") || tab.equals("UNKNOWN"))
                    continue;
                Double[] avgAndVariance = QueryUtil.getAvgRepetition(tab, tabCol.get(tab),line.getOptionValue(DATABASE.getOpt()));
                //表的平均基数和平均方差
                LOG.info(Arrays.toString(avgAndVariance));
                if(avgAndVariance.length == 2) {
                    LOG.info(String.format("表%s的平均基数：%s,平均基数方差：%s",
                            tab,avgAndVariance[0], avgAndVariance[1]));
                    res.put(tab + "_avg", avgAndVariance[0]);
                    res.put(tab + "_var", avgAndVariance[1]);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
            LOG.error("sql解析失败或计算重复率");
        }
        return sortFeature(res, size);
    }



    /**
     * 根据提交命令，找到数据集存放位置，统计数据集大小特征
     * @return
     */
    public List<Double> getMLFeature(){
        Double dataSize = 0d;
        try {
            Options options = getOptions();
            CommandLineParser parser = new DefaultParser( );
            CommandLine line = parser.parse(options,getArgs(cmd));

            String train = line.getOptionValue(TRAIN.getOpt());
            String test = line.getOptionValue(TEST.getOpt());
            if(train == null && test == null){
                train = line.getOptionValue(POINTS.getOpt());
                test = line.getOptionValue(CENTROIDS.getOpt());
            }
            //后面看看 直接改read函数
            dataSize = Double.valueOf(HdfsClient.readFileSize(train))+ HdfsClient.readFileSize(test);


        }catch (Exception e){
            e.printStackTrace();
            LOG.error("ML dataSet not exist!!!!");
        }
        return Arrays.asList(dataSize);
    }

    /**
     * 图数据特征提取
     * @return
     */
    public List <Double> getGraphFeature(){
        List<Double> res = new ArrayList<>();
        Double dataSize = 0d;
        try {
            Options options = getOptions();
            CommandLineParser parser = new DefaultParser( );
            CommandLine line = parser.parse(options,getArgs(cmd));

            String vertices = line.getOptionValue(VERTICES.getOpt());
            String edges = line.getOptionValue(EDGES.getOpt());
            if(vertices == null && edges == null){
                vertices = line.getOptionValue(PAGES.getOpt());
                edges = line.getOptionValue(LINKS.getOpt());
            }
            //数据集大小
            dataSize = Double.valueOf(HdfsClient.readFileSize(vertices))+ HdfsClient.readFileSize(edges);
            res.add(dataSize);
            //读取图文件转化为有向有环图
            Graph graph = GraphConverter.toGraph(new Path(vertices), new Path(edges));
            res.add(Double.valueOf(graph.getNodeNum()));
            res.add(Double.valueOf(graph.getEdgeNum()));
            res.add(Double.valueOf(graph.avgDegree()));
            res.add(Double.valueOf(graph.getCycleNum()));


        }catch (Exception e){
            e.printStackTrace();
            LOG.error("Graph Compute dataSet not exist!!!!");
        }

        return res;
    }

    public List<Double> getStreamFeature(){
        List<Double> res = new ArrayList<>();
        String path = cmd.split(" ")[0];
        String nexMarkConf = path.substring(0, path.length() -16) + "conf/nexmark.yaml";
        Double tps = getDouble(Config.getYaml(nexMarkConf).get("nexmark.workload.suite.10m.tps").toString());
        res.add(tps);
        return  res;
    }


    public List<Double> getFeature (String jobType){
        List<Double> res = new ArrayList<>();

        switch (jobType) {
            case "SQL":
                res = getSQLFeature();
                break;
            case "ML":
                res = getMLFeature();
                break;
            case "Graph":
                res = getGraphFeature();
                break;
            case "Stream":
                res = getStreamFeature();
                break;
            default:

        }
        return res;
    }

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

}
