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

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobExecute {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecute.class);
    private static final  int jobListen  = Config.getInt("job.listen.distance");
    public static String yarnIP = Config.getString("yarn.ip.port");
    public static int streamStableTime = Config.getInt("stream.stable.time");
    public static String pushgatewayDir = Config.getString("pushgateway.dir");

    private FlinkJob flinkJob = new FlinkJob("", "", new ArrayList<>(), "", 0d);
    private String submitPattern = Config.getString("job.submit.pattern");



    public int getNum(String str){
        return Integer.parseInt(str.replaceAll("[^0-9]", ""));

    }

//    /**
//     * 实验室集群使用
//     * @param url
//     * @return
//     * @throws Exception
//     */
//
    public JSONObject getJson( String url) throws Exception{
        HttpGet get = new HttpGet(url);
        CloseableHttpClient httpClient = HttpClients.custom().build();
        CloseableHttpResponse response = httpClient.execute(get);
        String json = EntityUtils.toString(response.getEntity(),"UTF-8");
       // LOG.info(json);
        return JSONObject.parseObject(json);
    }

//    /**
//     * 华为集群环境需要kerberos认证
//     * @param url
//     * @return
//     * @throws Exception
//     */
//
//    public JSONObject getJson(String url) throws Exception{
//        KerberosHuawei restTest = new KerberosHuawei(user, keytab, krb5Location, false);
//        HttpResponse response = restTest.callRestUrl(url, user);
//        String json = EntityUtils.toString(response.getEntity(),"UTF-8");
//        // System.out.println(json);
//        return JSONObject.parseObject(json);
//    }


    public void getJobStatus() {
        String status = "";
        try {
            LOG.info(String.format("job监听地址：http://%s/jobs", flinkJob.getWebUI()));
            JSONArray jobs = getJson(String.format("http://%s/jobs", flinkJob.getWebUI())).getJSONArray("jobs");
            if(jobs.size() > 0){
                LOG.info("正在运行job数量："+ jobs.size());
                for(int i = 0; i < jobs.size(); i++){
                    JSONObject job = jobs.getJSONObject(i);
                    String jobId = job.get("id").toString();
                    // System.out.println(id);
                    JSONObject result = getJson(String.format("http://%s/jobs/%s", flinkJob.getWebUI(), jobId));
                    status = result.getString("state");
                    flinkJob.setJobTime(result.getDouble("duration")/1000);
                    LOG.info(String.format("job状态：%s, job运行时间: %ss", status, flinkJob.getJobTime()));
                    if(status.equals("RESTARTING"))
                        status = "RUNNING";
                }
            }
        }catch (Exception e){
            LOG.error("监听job失败："+e.toString());
            if(submitPattern.equals("yarn-cluster")) {
                getApplicationStatus();
                status = flinkJob.getJobStatus();
            }
        }finally {
            if( status.equals("FINISHED")) {
                killJob(flinkJob);
                status = "SUCCEEDED";
            }
            if(!status.isEmpty())
                flinkJob.setJobStatus(status);
        }

    }

    /**
     * 获取application的状态
     */
    public void getApplicationStatus(){
        if(flinkJob.getApplicationId().isEmpty())
            return;
        Runtime run = Runtime.getRuntime();
        File wd = new File("/bin");
        Process proc = null;
        try {
            proc = run.exec("/bin/bash", null, wd);
            if (proc != null) {
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())), true);
                out.println("cd " + Config.getString("flink.dir"));
                out.println(String.format("yarn application -status %s", flinkJob.getApplicationId()));
                out.println("exit");
                String line;
                StringBuilder sb = new StringBuilder();
                Double startTime = 0d;
                Double endTime = 0d;
                while ((line = in.readLine()) != null) {
                    if(line.contains("Start-Time")){
                        startTime = Double.parseDouble(line.split(":")[1].trim());
                    }
                    if(line.contains("Finish-Time")){
                        endTime = Double.parseDouble(line.split(":")[1].trim());
                    }
                    if(line.contains("Final-State")){
                        flinkJob.setJobStatus(line.split(":")[1].trim());
                        flinkJob.setJobTime((endTime-startTime)/ 1000);
                        LOG.info("监听接口已关闭,任务执行结束，任务最终状态为"+ flinkJob.getJobStatus());
                    }
                    sb.append(line).append("\r\n");
                }
                proc.waitFor();
                in.close();
                out.close();
                proc.destroy();
            }
        } catch (Exception e) {
            LOG.error("IOException", e);
        }
//        String status = "";
//        try {
//            LOG.info(String.format("job监听地址：%s/ws/v1/cluster/apps/%s", yarnIP, flinkJob.getApplicationId()));
//            JSONObject app = getJson(String.format("%s/ws/v1/cluster/apps/%s", yarnIP, flinkJob.getApplicationId()))
//                    .getJSONObject("app");
//            if (app.size() > 0) {
//                status = app.getString("state");
//                if(!status.equals("RUNNING"))
//                    status = app.getString("finalStatus");
//                Double time = Double.valueOf(app.getInteger("elapsedTime"))/1000;
//                flinkJob.setJobTime(time);
//                LOG.info(String.format("job状态：%s, job运行时间: %ss", status, time));
//            }
//        }catch (Exception e){
//            LOG.error("监听job失败："+e.toString());
//           // status = "FAILED";
//        }finally {
//            flinkJob.setJobStatus(status);
//        }
    }

    /**
     * per job模式下得到tm_id
     * @return提供所有tmid, 因为tm动态增减
     * @throws Exception
     */
    public void getTMIdYarn(){
        //List<String> id = new ArrayList<>();
        LOG.info(String.format("taskmanager监听：http://%s/taskmanagers", flinkJob.getWebUI()));
        try {
            JSONArray tasks = getJson(String.format("http://%s/taskmanagers", flinkJob.getWebUI()))
                    .getJSONArray("taskmanagers");
            ArrayList<String> id = flinkJob.getTaskID();
            if (tasks.size() > 0) {
                for (int i = 0; i < tasks.size(); i++) {
                    JSONObject task = tasks.getJSONObject(i);
                    String t = task.get("id").toString();
                    if (!id.contains(t)) {
                        id.add(t);
                    }
                }
            }
            flinkJob.setTaskID(id);
        }catch (Exception e){
            LOG.error(String.format("监控taskmanager失败，作业状态：%s", flinkJob.getJobStatus()));
        }
    }

    /**
     * 在提交命令中修改配置，返回最终的flink作业提交命令
     * @param paras
     * @return
     */
    public String getSubmitCmd(Map<String, String> paras){
        String submit = Config.getString("job.submit.cmd");
        if(paras.isEmpty() || !submitPattern.equals("yarn-cluster"))
            return submit;

        int spilt = submit.length();
        if(submit.contains("yarn-cluster")){
            spilt = submit.indexOf("yarn-cluster") + "yarn-cluster".length();
        }
        StringBuilder cmd = new StringBuilder();
        cmd.append(submit.substring(0, spilt));
        for(String name : paras.keySet()){
            cmd.append(" ");
            String value = paras.get(name);
            if(name.equals("taskmanager.memory.process.size")){
                cmd.append(String.format("-ytm %s", getNum(value) * 1024 +"m"));
                continue;
            }
            if(name.equals("taskmanager.numberOfTaskSlots")){
                cmd.append(String.format("-ys %d", Integer.parseInt(value)));
                continue;
            }
            if(name.equals("parallelism.default")){
                cmd.append(String.format("-p %d", Integer.parseInt(value)));
                continue;
            }
            if(name.equals("taskmanager.memory.network.fraction")){
                cmd.append(String.format("-yD %s=%s", name, Double.parseDouble(value)));
                continue;
            }
            cmd.append(String.format("-yD %s=%s", name, value));
        }

        cmd.append(submit.substring(spilt));
        return cmd.toString();
    }


    /**
     * 模式1：向yarn上提交作业，并获取作业信息
     * 模式2：向flink cluster上提交作业，并获取作业信息
     */
    public void submitJob(Map<String, String> paras) {
        //java Runtime实现linux集群上的shell命令执行
        Runtime run = Runtime.getRuntime();
        File wd = new File("/bin");
        String flinkInterface = "";
        String applicationId = "";
        Process proc;
        try {
            LOG.info("开始向yarn提交任务");
            proc = run.exec("/bin/bash", null, wd);
            if (proc != null) {
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())), true);
                out.println("cd " + Config.getString("flink.dir"));
                if(flinkJob.getJobMode().equals("stream"))
                    out.println(String.format("sed -i \"/^  parallelism/c\\  parallelism: %s\" conf/sql-client-defaults.yaml",
                            paras.get("parallelism.default")));
                if(!submitPattern.equals("yarn-cluster") && !paras.isEmpty()){
                    if (!Config.modifyFlinkConf(paras)){
                        LOG.error("修改参数失败~~，无法提交任务");
                        return;
                    }
                }
                //根据参数，生成flink作业的提交命令
                String cmd = getSubmitCmd(paras);
                LOG.info("提交作业：" + cmd);
                out.println(cmd);
                //这个命令必须执行，否则in流不结束。shell脚本中有echo或者print输出， 会导致缓冲区被用完，程序卡死! 为了避免这种情况，
                //一定要把缓冲区读一下 reader.readLine() 读出即可，如果需要输出的话可以使用StringBuilder进行拼接然后输出
                out.println("exit");
                String line;
                StringBuilder sb = new StringBuilder();
                while ((line = in.readLine()) != null){
                    LOG.info(line);
                    if (line.contains("error") || line.contains("ERROR")) {
                        LOG.info("作业提交失败~~~");
                        break;
                    }
                    if (submitPattern.equals("yarn-cluster") && line.contains("Found Web Interface")) {
                        LOG.info("作业提交成功！！！");
                        //从提交成功的日志中提取出applicationID和flink 的webUI
                        flinkInterface = line.substring(line.indexOf("Found Web Interface") + "Found Web Interface".length(), line.indexOf("of")).trim();
                        Pattern pattern = Pattern.compile("\'(.*?)\'");
                        Matcher matcher = pattern.matcher(line);
                        if (matcher.find()) {
                            applicationId = matcher.group(0).replace("\'", "");
                            LOG.info(String.format("获取任务id:%s", applicationId));
                        }
                        sb.append(line).append("\r\n");
                        //提交成功后，更新flinkJob信息
                        flinkJob.setJobStatus("RUNNING");
                        flinkJob.setApplicationId(applicationId);
                        flinkJob.setWebUI(flinkInterface);
                        break;
                    }
                    if(!submitPattern.equals("yarn-cluster") && (line.contains("Job has been submitted")
                            || line.contains("Start to run query"))){
                        LOG.info("作业提交成功！！！");
                        flinkJob.setJobStatus("RUNNING");
                        flinkJob.setWebUI("slave033:8081");
                        flinkJob.setApplicationId("0");
                        getTMIdYarn();
                        break;
                    }
                    sb.append(line).append("\r\n");
                }
                //跳出后把提交日志打印到框架日志文件中
                //LOG.info(sb.toString());
                in.close();
                out.close();
                proc.destroy();
            }

        } catch (Exception e) {
            LOG.error("任务提交失败：" + e.toString());
        }
    }



    public FlinkJob executeJob(Map<String, String> paras){
        //提交作业前, 重启pushgateway
        restartPushgateway();
        //根据参数配置，提交作业，
        submitJob(paras);
        if(flinkJob.getApplicationId().isEmpty() || flinkJob.getWebUI().isEmpty()){
            LOG.error("作业提交失败，application为空或者没有获取到Flink Web UI~~~");
            return flinkJob;
        }
        //循环监控作业，直到作业执行结束
        //针对流作业，当作业执行时间达到稳定执行时间，认为流作业成功执行完毕（实际上flink作业依然在执行）
        try{
            while (flinkJob.getJobStatus().equals("RUNNING")) {
                //在执行中从flink webUI获取出现过的taskMananger ID
                if(submitPattern.equals("yarn-cluster"))
                    getTMIdYarn();
                //更新作业当前状态（RUNNING/FAILED/FINISHED），作业执行时间
                getJobStatus();
                //根据设置的监听间隔睡眠
                TimeUnit.SECONDS.sleep(jobListen);
                //避免更新失败
                //当作业完成依然在yran上保持running的可以再次尝试调用getJobStatus

//                if(flinkJob.getJobStatus().equals("")){
//                    LOG.warn(String.format("尝试再次连接~~"));
//                    getJobStatus();
//                }
                if(flinkJob.getJobTime() < 0)
                    LOG.warn(String.format("获取job执行时间错误，当前作业执行时间为：%s", flinkJob.getJobTime()));
                //我的集群测试用的
                //  if(flinkJob.getJobTime() > 100) {
                //      killJob(flinkJob);
                //      getApplicationStatus();
                //      flinkJob.setJobStatus("SUCCEEDED");
                //  }
                //判断流作业是否达到稳定
                if(flinkJob.getJobMode().equals("stream") && (flinkJob.getJobTime() > streamStableTime*60)){
                    LOG.info("流任务达到稳定状态时间~~");
                    flinkJob.setJobStatus("SUCCEEDED");
                }
            }
        }catch (Exception e){
            LOG.error("监听失败："+e.toString());
        }finally {
            //监听完毕后，返回作业执行信息
            if(flinkJob.getJobStatus().equals("FAILED"))
                killJob(flinkJob);
            return flinkJob;
        }
    }



    public FlinkJob executeJobMulti(List<Map<String, String>> paras){
        //提交作业前, 重启pushgateway
        restartPushgateway();
        //根据参数配置，提交作业，
        for(Map<String, String> para : paras) {
            submitJob(para);
        }
        if(flinkJob.getApplicationId().isEmpty() || flinkJob.getWebUI().isEmpty()){
            LOG.error("作业提交失败，application为空或者没有获取到Flink Web UI~~~");
            return flinkJob;
        }
        //循环监控作业，直到作业执行结束
        //针对流作业，当作业执行时间达到稳定执行时间，认为流作业成功执行完毕（实际上flink作业依然在执行）
        try{
            while (flinkJob.getJobStatus().equals("RUNNING")) {
                //在执行中从flink webUI获取出现过的taskMananger ID
                if(submitPattern.equals("yarn-cluster"))
                    getTMIdYarn();
                //更新作业当前状态（RUNNING/FAILED/FINISHED），作业执行时间
                getJobStatus();
                //根据设置的监听间隔睡眠
                TimeUnit.SECONDS.sleep(jobListen);
                //避免更新失败
                //当作业完成依然在yran上保持running的可以再次尝试调用getJobStatus

//                if(flinkJob.getJobStatus().equals("")){
//                    LOG.warn(String.format("尝试再次连接~~"));
//                    getJobStatus();
//                }
                if(flinkJob.getJobTime() < 0)
                    LOG.warn(String.format("获取job执行时间错误，当前作业执行时间为：%s", flinkJob.getJobTime()));
                //我的集群测试用的
                //  if(flinkJob.getJobTime() > 100) {
                //      killJob(flinkJob);
                //      getApplicationStatus();
                //      flinkJob.setJobStatus("SUCCEEDED");
                //  }
                //判断流作业是否达到稳定
                if(flinkJob.getJobMode().equals("stream") && (flinkJob.getJobTime() > streamStableTime*60)){
                    LOG.info("流任务达到稳定状态时间~~");
                    flinkJob.setJobStatus("SUCCEEDED");
                }
            }
        }catch (Exception e){
            LOG.error("监听失败："+e.toString());
        }finally {
            //监听完毕后，返回作业执行信息
            if(flinkJob.getJobStatus().equals("FAILED"))
                killJob(flinkJob);
            return flinkJob;
        }
    }



    /*
    yarn模式kill作业
    cluster模式不用kill 提交命令时会重启flink集群
     */
    public void killJob( FlinkJob job ){
        if(job.getApplicationId().isEmpty() || !submitPattern.equals("yarn-cluster"))
            return;
        Runtime run = Runtime.getRuntime();
        File wd = new File("/bin");
        Process proc = null;
        try {
            proc = run.exec("/bin/bash", null, wd);
            if (proc != null) {
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())), true);
                out.println("cd " + Config.getString("flink.dir"));
                out.println(String.format("yarn application -kill %s", job.getApplicationId()));
                out.println("exit");
                String line;
                StringBuilder sb = new StringBuilder();
                while ((line = in.readLine()) != null) {
                    sb.append(line).append("\r\n");
                }
                proc.waitFor();
                in.close();
                out.close();
                proc.destroy();
            }
        } catch (Exception e) {
            LOG.error("IOException", e);
        }
    }

    /**
     * 针对我的环境 每次执行完清理pushgateway
     */
    public void restartPushgateway(){
        Runtime run = Runtime.getRuntime();
        File wd = new File("/bin");
        Process proc = null;
        try {
            proc = run.exec("/bin/bash", null, wd);
            if (proc != null) {
                BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())), true);
                out.println("cd " + pushgatewayDir);
                out.println("./restart.sh");
                out.println("exit");//这个命令必须执行，否则in流不结束。
                //shell脚本中有echo或者print输出， 会导致缓冲区被用完，程序卡死! 为了避免这种情况，
                //一定要把缓冲区读一下 reader.readLine() 读出即可，如果需要输出的话可以使用StringBuilder
                //进行拼接然后输出
                String line;
                StringBuilder sb = new StringBuilder();
                while ((line = in.readLine()) != null) {
//                    if(line.contains(String.format("Killed application %s", applicationId))
//                            || line.contains(String.format("%s has already finished", applicationId)))
//                        isKilled = true;
                    sb.append(line).append("\r\n");
                }
                LOG.info(sb.toString());
                proc.waitFor();
                in.close();
                out.close();
                proc.destroy();
            }
        } catch (Exception e) {
            LOG.error("IOException", e);
        }
    }
}
