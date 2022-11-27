package prediction;

import common.GraphConverter;
import util.Config;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import graph.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*对job submit 做特征解析
1 转化为dag
2 图特征
3 算子数统计
 */
public class JobFeature {

    private static final Logger LOG = LoggerFactory.getLogger(JobFeature.class);
    private String cmd = Config.getString("job.submit.cmd").
            replace("-m yarn-cluster", "").replace("run","info");




    /*
    将string追加到hdfs的文件中
     */
    public void writeHDFS(){
        String hdfs_path = "hdfs://slave033:9000/movie/data/ratings/ratings.txt";
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(URI.create(hdfs_path), conf);
            OutputStream output = fs.append(new Path(hdfs_path));
            output.write("8,23173,3.0,11112340808".getBytes("UTF-8"));
            output.write("\n".getBytes("UTF-8"));//换行
            fs.close();
            output.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    /**
     * 通过flink info 命令直接获取作业DAG图的Json格式数据
     * 可能一个大作业包含多个子任务，因此有多个逻辑执行图，返回DAG列表
     * @return
     */
    public List<JSONArray> getGraphs(){
        List<JSONArray> graphs = new ArrayList<>();
        Runtime run = Runtime.getRuntime();
        File wd = new File("/bin");
        JSONArray nodes = new JSONArray();
        Process proc;
        try {
            proc = run.exec("/bin/bash", null, wd);
            if (proc != null) {
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())), true);
                out.println("cd " + Config.getString("flink.dir"));
                //根据提交命令,
                LOG.info("作业执行命令：" + cmd);
                out.println(cmd);
                out.println("exit");

                String line;
                boolean isAppend = false;
                StringBuilder sb = new StringBuilder();
                while ((line = in.readLine()) != null) {
                    if(isAppend && line.equals("--------------------------------------------------------------")){
                        JSONObject jsonObject = JSONObject.parseObject(sb.toString());
                        graphs.add(jsonObject.getJSONArray("nodes"));
                        sb.delete(0, sb.length());
                        isAppend = false;
                    }else if(isAppend)
                        sb.append(line).append("\n");
                    if(line.contains("Execution Plan"))
                        isAppend = true;

                }
                in.close();
                out.close();
                proc.destroy();
            }
        } catch (Exception e) {
            LOG.error("读取文件数据异常" ,e);
        }finally {
            return graphs;
        }
    }


    private static String streamToString(InputStream inputStream) {
        BufferedInputStream in = new BufferedInputStream(inputStream);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        try {
            int c;
            while ((c = in.read()) != -1) {
                outStream.write(c);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
        return new String(outStream.toByteArray(), StandardCharsets.UTF_8);
    }

    /*
    根据DAG建立图结构数据
    返回sql作业DAG特征
     */
    private List<Double> getSQLFeature(JSONArray graph) {

        Graph dag = GraphConverter.jsonToDAG(graph);
        //路径数
        int pathNum = dag.gePathNum();
        //最短路径
        int minPath = dag.minPath();
        //图深度
        int depth = dag.maxDepth();
        //平均度
        Double avgDegree = dag.avgDegree();

        int join = 0, sort = 0, groupBy = 0;
        int hash = 0, broadCast = 0, global = 0;
        for(Node node : dag.getNodes()) {
           String pact = node.label.getPact().toLowerCase();
           String content = node.label.getContent().toLowerCase();
           List<Edge> edges = dag.getEdges(node);
           //获取每个节点的 输出边集合  统计传输策略
           for(Edge edge : edges){
               String shipStrategy = edge.weight.toLowerCase();
               if(shipStrategy.contains("hash") || shipStrategy.contains("keygroup"))
                   hash ++;
               if(shipStrategy.contains("broadcast"))
                   broadCast++;
               if(shipStrategy.contains("global"))
                   global++;

           }
           if (pact.contains("join") || content.contains("join"))
               join++;
           if (pact.contains("sort") || content.contains("sort"))
               sort++;
           if (pact.contains("aggregate") || content.contains("aggregate"))
               groupBy++;
           if (pact.contains("groupby") || content.contains("groupby"))
               groupBy++;

        }
        LOG.info(String.format("Join节点数:%s, Sort节点数:%s, GroupBy节点数:%s，Hash传输边数：%s, BroadCast传输边数：%s，Global传输边数:%s"
                        + "图深度：%s, 平均度：%s,路径数：%s, 最短路径长度：%s",
                join, sort, groupBy,hash, broadCast, global, depth, avgDegree,pathNum, minPath));
        List<Double> feature = new ArrayList<>();
        Collections.addAll(feature,Double.valueOf(join),Double.valueOf(sort), Double.valueOf(groupBy),
                Double.valueOf(hash), Double.valueOf(broadCast), Double.valueOf(global),
                Double.valueOf(depth),Double.valueOf(avgDegree), Double.valueOf(pathNum), Double.valueOf(minPath));

        return  feature;
    }



    private List<Double> getMLFeature(JSONArray graph) {

        Graph dag = GraphConverter.jsonToDAG(graph);
        //路径数
        int pathNum = dag.gePathNum();
        //最短路径
        int minPath = dag.minPath();
        //图深度
        int depth = dag.maxDepth();
        //平均度
        Double avgDegree = dag.avgDegree();

        int map = 0, reduce = 0, iterate = 0;
        int hash = 0, broadCast = 0, global = 0;
        for(Node node : dag.getNodes()) {
            String pact = node.label.getPact();
            String content = node.label.getContent();
            List<Edge> edges = dag.getEdges(node);
            //获取每个节点的 输出边集合  统计传输策略
            for(Edge edge : edges){
                String shipStrategy = edge.weight.toLowerCase();
                if(shipStrategy.contains("hash") || shipStrategy.contains("keygroup"))
                    hash ++;
                if(shipStrategy.contains("broadcast"))
                    broadCast++;
                if(shipStrategy.contains("global"))
                    global++;

            }

            if (pact.contains("Map") || content.contains("Map"))
                map++;
            if (pact.contains("Reduce") || content.contains("Reduce"))
                reduce++;
            if (pact.contains("Iterate") || content.contains("Iterate"))
                iterate++;
        }
        LOG.info(String.format("Map节点数:%s, Reduce节点数:%s, Iterate节点数:%s，Hash传输边数：%s, BroadCast传输边数：%s，Global传输边数:%s"
                        + "图深度：%s, 平均度：%s,路径数：%s, 最短路径长度：%s",
                map, reduce, iterate,hash, broadCast, global, depth, avgDegree,pathNum, minPath));
        List<Double> feature = new ArrayList<>();
        Collections.addAll(feature,Double.valueOf(map),Double.valueOf(reduce), Double.valueOf(iterate),
                Double.valueOf(hash), Double.valueOf(broadCast), Double.valueOf(global),
                Double.valueOf(depth),Double.valueOf(avgDegree), Double.valueOf(pathNum), Double.valueOf(minPath));

        return  feature;
    }


    public List<List<Double>> getFeature(String jobType){
        List<JSONArray> graphs = getGraphs();
        LOG.info("DAG个数" + graphs.size());
        List<List<Double>> res = new ArrayList<>();
        for(JSONArray graph : graphs) {
            //实验的测试集时 Stream SQL 其他流式作业特征为default
            switch (jobType) {
                case "SQL":
                case "Stream SQL":
                    res.add(getSQLFeature(graph));
                    break;
                case "ML":
                case "Graph":
                    res.add(getMLFeature(graph));
                    break;
                default:

            }
        }
        return res;

    }

}
