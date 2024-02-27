package analysis;

import util.Config;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class GetMetrics {
    private static final Logger LOG = LoggerFactory.getLogger(GetMetrics.class);
    private static final String prometheusIP = Config.getString("prometheus.ip.port");
    public static final String jobMode = Config.getString("flink.job.mode");
    public static final String yarnIP = Config.getString("yarn.ip.port");
    public static final int sampleTimes = Config.getInt("stream.sample.times");
    public static final int streamDistance = Config.getInt("stream.listen.distance");

    public  int distance;
    private double benchMarkTime;
    public  Map<String, String> queryResult;
    public  double reduceRatio;
    public  Double maxLatency;
    public  Double maxConsumption;
    public  List<String> jobLatency;
    public  List<String> jobConsumption;

    public GetMetrics() {
        this.distance = Config.getInt("query.distance");
        this.queryResult = new LinkedHashMap<>();
        this.jobLatency = new ArrayList<>();
        this.jobConsumption = new ArrayList<>();
        if (jobMode.equals("batch")) {
            this.reduceRatio = Double.valueOf(Config.getString("time.reduce.ratio"));
            try {
                this.benchMarkTime = getBenchMarkTime();
                LOG.info(String.format("基准时间：%ss", benchMarkTime));
                System.out.println(String.format("默认配置执行时间：%ss", benchMarkTime));
            } catch (Exception e) {
                LOG.error(e.toString());
            }
        }
        if(jobMode.equals("stream")){
            this.maxLatency = Double.valueOf(Config.getInt("max.latency"));
            this.maxConsumption = Double.valueOf(Config.getInt("consumption.target"));
        }
    }



    public Map<String, String> getQueryResult(){
        return queryResult;
    }

    public double getBenchMarkTime(){
        //开始监听job是否执行结束或者失败
        JobExecute jobSubmit = new JobExecute();
        FlinkJob initialJob;
        LinkedHashMap<String, String> initialConfig = new LinkedHashMap<>();
        initialConfig.put("taskmanager.memory.process.size", "2g");
        initialConfig.put("taskmanager.numberOfTaskSlots", "4");
        initialConfig.put("taskmanager.memory.managed.fraction", "0.4");
        initialConfig.put("taskmanager.memory.network.fraction", "0.1");
        initialConfig.put("parallelism.default", "1");
        while (true) {
            initialJob = jobSubmit.executeJob(initialConfig);
            if(initialJob.getJobStatus().equals("SUCCEEDED")) {
                LOG.info("end of initial job!");
                break;
            }
            else {
                LOG.error("The default configuration parameter task execution failed, continue to execute the default configuration");
//                initialConfig.put("taskmanager.memory.process.size", "4g");
//                initialConfig.put("taskmanager.numberOfTaskSlots", "8");
                initialConfig.put("parallelism.default", "4");
            }
        }
        return initialJob.getJobTime();
    }

    public JSONObject getJson( String url) throws Exception{
        HttpGet get = new HttpGet(url);
        CloseableHttpClient httpClient = HttpClients.custom().build();
        CloseableHttpResponse response = httpClient.execute(get);
        String json = EntityUtils.toString(response.getEntity(),"UTF-8");
        return JSONObject.parseObject(json);
    }


    public static  <T> T get(Class<T> clz,Object o){
        if(clz.isInstance(o)){
            return clz.cast(o);
        }
        return null;
    }

    /**
     * 计算指标总评分（默认每个指标的基准是1）
     * 查询总分数/基准总分数
     * @param metrics
     * @return
     */

    public  Double getTotalScore( Map<String,Double> metrics, Map<String, Double> queryResult ){
        Double score = 0d;
        LOG.info("Parameter Performance Metrics Score：");
        for(String key : metrics.keySet()){
            Double temp = metrics.get(key);
            if( temp <= 0 || !queryResult.containsKey(key))
                continue;
           // benchMarkScore += temp;
            //查询结果*指标权重
            try {
                Double value =  queryResult.get(key);
                //批作业
                if(key.equals("time"))
                    value = (benchMarkTime - queryResult.get(key))/(benchMarkTime * reduceRatio);
                //流作业
                if(key.equals("t1"))
                    value = 0d;
                score += value * metrics.get(key);
                LOG.info(String.format("%s: %s", key, value*metrics.get(key)));

            }catch (Exception e){
                LOG.error(e.toString());
            }
        }
        return score;
    }


    /**
     * 批作业
     * 通过读取配置文件里的metrics，查询再时间范围内最大指标值,并根据配置的权值计算综合性能评分(修)
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    public Double getMetric(String start, String end, FlinkJob job){
        //执行中申请过的taskManagerID集合
        List<String> taskId = job.getTaskID();
        LOG.info(Arrays.toString(taskId.toArray()));
        if(taskId.isEmpty()){
            LOG.error("query taskmanager fail!");
            return 0d;
        }
        //配置文件中的 性能指标：权重
        Map<String, Double> metrics =  Config.getMetrics();
        //存储查询结果
        HashMap<String, Double> querys = new HashMap<>();
        for(String query : metrics.keySet()){
            //筛掉 流作业指标
            if(query.equals("latency") || query.equals("consumption.ratio"))
                continue;
            querys.put(query, 0d);
            if(query.equals("time")){
                Double time = job.getJobTime();
                querys.put(query, time);
                continue;
            }
            //在Prometheus中查询时间段内的性能指标结果
            JSONArray result = queryPrometheus(start, end, query);
            if (result.isEmpty()){
                LOG.error(query + "query result is empty");
                continue;
            }
            //根据taskID 筛选本次执行结果（prometheus会参杂其他很多job）
            LOG.info("Before selecting："+ result.size());
//            for (int i = 0; i < result.size(); i++) {
//                Object json = result.get(i);
//                if (json instanceof JSONObject) {
//                    String tmId = ((JSONObject) json).getJSONObject("metric").get("tm_id").toString();
//                    if (!taskId.contains(tmId)) {
//                        result.remove(i);
//                        i--;
//                    }
//                }
//            }
            LOG.info(String.format("After selecting：%s, number of tm：%s", result.size(), taskId.size()));
            if(result.isEmpty()){
                LOG.error("task id not found");
                continue;
            }
            //计算性能指标的平均值
            Double average = getAvg(result);
            querys.put(query, average);
        }
        //展示所有性能指标的查询结果
        for(String key : querys.keySet()){
            String result = key.equals("time") ? String.format("%ss", querys.get(key)) : querys.get(key).toString();
            LOG.info(key +":"+ result);
            queryResult.put(key, result);
        }
        //根据权重和查询结果计算性能总分= ∑性能指标评分∗ 权重
        return getTotalScore(metrics, querys);
    }


    public JSONArray queryPrometheus(String start, String end, String query){
        //从prometheus库中查询时间段内性能指标的时序数据
        JSONObject data = new JSONObject();
        try {
            String url = String.format("%s/api/v1/query_range?query=%s&start=%s&end=%s&step=%d",
                    prometheusIP, query, start, end, distance);
            LOG.info(String.format("prometheus data url:%s",url));
            data = getJson(url).getJSONObject("data");


        }catch (Exception e){
            LOG.error("query prometheus fail："+e);
        }finally {
            if (data.isEmpty()) {
                LOG.error(String.format("connect %s fail", prometheusIP));
                return new JSONArray();
            }
            return data.getJSONArray("result");
        }
    }

    /**
     * 计算一个TM一个性能指标下不同时间点的平均值
     * @param result
     * @return
     */
    public Double getAvg(JSONArray result){
        Double average = 0d;
        for (Object josn : result) {
            List<Object> value = ((JSONObject) josn).getJSONArray("values");
           // LOG.info("values:"+ value);
            Double sum = 0.0;
            for (Object v : value) {
                Object rate = get(List.class, v).get(1);
                if (rate instanceof String) {
                    sum += Math.abs(Double.valueOf((String) rate));
                }
            }
            average += sum/value.size();
        }
        return average / result.size();
    }

    /**
     * 查询源算子的verticesID,为了后续查询源算子的消费速率
     * @param flinkWeb
     * @return
     */
    public List<String> getSourceVertices(String flinkWeb){
        List<String> sourceVertices = new ArrayList<>();
        if(flinkWeb.isEmpty())
            return sourceVertices;

        String url = String.format("http://%s/jobs", flinkWeb);
        //String url = String.format("%s/proxy/%s/jobs",yarnIP, flinkWeb);
        LOG.info("flink job url："+ url );
        try {
            JSONArray jobs = getJson(url).getJSONArray("jobs");
            if(jobs.size() > 0){
                for (int i = 0; i < jobs.size(); i++){
                    String jobID = jobs.getJSONObject(i).get("id").toString();
                    String sourceURL = String.format("%s/%s", url, jobID);
                    JSONArray vertices = getJson(sourceURL).getJSONArray("vertices");
                    if(vertices.isEmpty())
                        continue;
                    for(int j = 0; j < vertices.size(); j++){
                        String name = vertices.getJSONObject(i).get("name").toString();
                        String id = vertices.getJSONObject(i).get("id").toString();
                        if (name.contains("Source") && !sourceVertices.contains(id)) {
                             sourceVertices.add(id);
                        }
                    }
                }
            }
        }catch (Exception e){
            LOG.error("get flink Source fail："+e);
        }finally {
            return sourceVertices;
        }
    }

    /**
     * 流作业的性能指标查询和计算
     * @param start
     * @param job
     * @return
     * @throws Exception
     */

    public Double getMetric(String start, FlinkJob job) throws Exception{
        String queryTime = start;
        //获取源算子ID集合
        List<String> sourceID = getSourceVertices(job.getWebUI());
        LOG.info(Arrays.toString(sourceID.toArray()));
        //记录性能综合评分
        Double totalScore = 0d;
        Double latency = 0d;
        Double consume = 0d;
        Double consumeScore = 0d;
        Double latencyScore = 0d;
        if(!sourceID.isEmpty()) {
            //配置文件中的 性能指标：权重
            Map<String, Double> metrics =  Config.getMetrics();
            String query;
            JSONArray result;
            //间隔采样，采样次数由sampleTimes决定
            for (int i = 0; i < sampleTimes; i++) {
                LOG.info(String.format("Obtain the delay indicators and consumption indicators of the streaming job at the %s sampling time", i+1));
                //对多个源算子统计消费速率
                for(String id : sourceID){
                    if (metrics.containsKey("consumption.ratio")) {
                        //old
                        //query = String.format("flink_taskmanager_job_task_operator_%s_numRecordsOutPerSecond", id);
                        query = "flink_taskmanager_job_task_operator_numRecordsOutPerSecond";
                        //查询采样时间点的性能指标
                        result = queryPrometheus(queryTime, queryTime, query);
                        //LOG.info(result.toJSONString());
                        if (result.isEmpty()) {
                            LOG.error(query + "result is empty");
                            continue;
                        }
                        //根据查询结果计算平均值
                        consume += getAvg(result);
                    }
                }
                if (metrics.containsKey("latency")) {
                    query = "flink_taskmanager_job_latency_source_id_operator_id_operator_subtask_index_latency";
                    result = queryPrometheus(queryTime, queryTime, query);
                    // LOG.info(result.toJSONString());
                    if (result.isEmpty()) {
                        LOG.error(query + "query result is empty");
                        continue;
                    }
                    latency += getAvg(result);
                }
                TimeUnit.MINUTES.sleep(streamDistance);
                //bug
                //queryTime = String.valueOf(System.currentTimeMillis()-16*60*60*1000).substring(0,10);
                queryTime = String.valueOf(System.currentTimeMillis()).substring(0,10);
            }
            //计算平均消费速率 和 性能评分
            consume /= sampleTimes*sourceID.size();
            if(consume > maxConsumption)
                LOG.warn("The current consumption rate has exceeded the maximum consumption rate~~");
            consumeScore = (consume > maxConsumption ?  1 : consume/maxConsumption) * metrics.get("consumption.ratio");
            totalScore += consumeScore;
            //计算平均延迟时间 和 性能评分
            latency = Double.valueOf(String.format("%.4f", latency /(1000*sampleTimes)));
            latencyScore = (1 - latency/ maxLatency)* metrics.get("latency") ;
            if(latency > maxLatency)
                LOG.warn("The current delay has exceeded the maximum delay time~~");
            totalScore += latencyScore;
        }
        queryResult.put("consumption.ratio", String.format("%s record/per second",consume));
        queryResult.put("latency", String.format("%ss",latency));
        jobLatency.add(String.format("%ss",latency));
        jobConsumption.add(String.format("%s record/per second",consume));
        //bug
        //String end = String.valueOf(System.currentTimeMillis()-16*60*60*1000).substring(0,10);
        String end = String.valueOf(System.currentTimeMillis()).substring(0,10);
        //计算从作业结束到现在时间段的其他性能指标
        totalScore += getMetric(start, end, job);
        LOG.info(String.format("consumption.ratio：%s record/per second, performance score：%s", consume, consumeScore));
        LOG.info(String.format("latency：%ss, performance score：%s", latency, latencyScore));
        return totalScore;
    }


}
