package search;

import prediction.LGBMPredict;
import util.Config;
import analysis.FlinkJob;
import analysis.GetMetrics;
import analysis.JobExecute;
import util.PrintResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeneticSearch {
    private static final Logger LOG = LoggerFactory.getLogger(GeneticSearch.class);

    private int ChrNum ; // 染色体数量
    private String[] ipop; // 一个种群中染色体总数
    public final int GENE ; // 基因数
    private double bestfitness = Integer.MIN_VALUE; // 个体的适应值=作业性能综合评分
    public Map<String, String> bestResult = new LinkedHashMap<>();
    private String bestStr; // 当前最优染色体
    private Map<String, String> bestConf;  // 最优解的染色体的参数配置
    private Map<String, String[]> paras;  // 参数名和参数值
    private Map<String, Integer> parasIndex;  // 参数在染色体上的位置
    private Boolean isEvolution = false;
    private GetMetrics getMetrics;
    private int failedCount = 0;  // flink执行失败任务数
    private final Map<String, Double> allResult = new LinkedHashMap<>();
    private int iterateCount = 0;
    private final ArrayList<String> jobTimes = new ArrayList<>();  // 记录作业执行时间变化
    private final ArrayList<Double> jobScore = new ArrayList<>();  // 记录种群性能评分变化
    private final ArrayList<Double> totalScore = new ArrayList<>();  // 记录个体性能评分变化

    public GeneticSearch(Map<String, String[]> paras){
        this.paras = paras;
        this.parasIndex = new LinkedHashMap<>();
        this.GENE = getGeneNum(paras);
        this.ChrNum = paras.size()+1;
        //根据参数预选结果初始化种群
        this.ipop = primaryPop();
        mutation();
        this.getMetrics =  new GetMetrics();
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
    public static Integer getInt(String str){
        return Integer.parseInt(str.replaceAll("[^0-9]", ""));
    }


    public int getGeneNum(Map<String, String[]> paras){
        int geneNum = 0;
        for(String para : paras.keySet()){
            geneNum += Integer.toBinaryString(paras.get(para).length-1).length();
            //记录每个参数在染色体上最后一个位置点
            parasIndex.put(para, geneNum);
        }
        LOG.info("染色体长度："+geneNum);
        return geneNum;
    }


    /**
     * 种群初始化：根据参数预选结果，初始化种群
     */
    private String[] primaryPop() {
        String[] ipop = new String[ChrNum];
        LGBMPredict lgbm = new LGBMPredict();
        String temp = configToStr(lgbm.getParameter());
        for (int i = 0; i < ChrNum; i++) {
            ipop[i] = temp;
        }
        return ipop;
    }

    /**
     * 个体编码：将一组参数配置转换成染色体
     * @param config
     * @return
     */
    public String configToStr(Map<String, String> config){
        StringBuilder sb = new StringBuilder();
        int start = 0;
        for (String name : config.keySet()) {
            int num = match(paras.get(name), config.get(name));
            int end = parasIndex.get(name);
            String str = Integer.toBinaryString(num);
            for (int i = 0; i < end-start-str.length(); i++){
                sb.append("0");
            }
            sb.append(str);
            start = end;
        }
        return sb.toString();
    }

    public int match(String[] nums, String target){
        Double poor = Double.MAX_VALUE;
        int i = 0;
        for (; i < nums.length; i++){
            Double temp = Math.abs(getDouble(target)-getDouble(nums[i]));
            if(temp> poor)
                break;
            poor = temp;
        }
        //LOG.info(String.format("%s,%s,%s", Arrays.toString(nums), target, nums[i-1]));
        if(i < 0 ) return i;
        if(i > nums.length) return nums.length-1;
        return i-1;
    }

    /**
     * 个体解码：将染色体转化为一组参数配置
     * @param str
     * @return
     */
    public Map<String, String> strToConfig(String str){
        Map<String, String> config = new LinkedHashMap<>();
        int start = 0;
        for (String para : paras.keySet()) {
            //从parasIndex（参数与染色体的映射）中，获得参数的位置，并将基因段的二进制转换成十进制
            int index = Integer.parseInt(str.substring(start, parasIndex.get(para)), 2);

            start = parasIndex.get(para);
            config.put(para, paras.get(para)[index]);
        }
        return config;
    }

    /**
     * 可行性剪枝：判断个体（参数配置）是否符合规则
     * @param str（个体）
     * @return
     */
    public boolean rule(String str){
        if (str.length() != GENE) {
            LOG.error(String.format("%s的染色体长度不匹配", str));
            return false;
        }
        //判断变异后的个体是否在参数取值范围内
        int start = 0;
        for(String para : paras.keySet()){
            String gene = str.substring(start, parasIndex.get(para));
            int index = Integer.parseInt(str.substring(start, parasIndex.get(para)), 2);
            start = parasIndex.get(para);
            if (index >= paras.get(para).length) {
                LOG.error(String.format("%s的基因%s不在参数%s的取值范围内", str, gene, para));
                return false;
            }
        }
        //将个体转化为参数配置
        Map<String, String> config = strToConfig(str);
        return ParameterRule.rule(config);
    }

    /**
     * 适应度计算
     */
    private double[] calculateFitness( ) {
        double[] populationFitness = new double[ChrNum];

        for(int i = 0; i < ChrNum; i++){
            String str = ipop[i];
            double fitness = 0d;

            //记忆化剪枝
            if(allResult.containsKey(str)) {
                populationFitness[i] = allResult.get(str);
                LOG.info(String.format("参数配置%s已运行过,适应度计算结果%s", str, allResult.get(str)));
                continue;
            }

            //个体解码，转化为参数配置
            Map<String, String> newConfig = strToConfig(str);
            iterateCount++;
            try {
                String startTime = String.valueOf(System.currentTimeMillis()).substring(0,10);
                JobExecute jobSubmit = new JobExecute();

                //把参数加入Flink提交命令中，在线运行作业
                FlinkJob job = jobSubmit.executeJob(newConfig);
                //运行结果
                if(job.getJobStatus().equals("SUCCEEDED")) {
                    LOG.info("作业结束！！！");
                    LOG.info(String.format("第%s次搜索参数配置：", iterateCount));
                    for(String name : newConfig.keySet()){
                        LOG.info(String.format("%s: %s",name, newConfig.get(name)));
                    }
                    jobTimes.add(String.format("%.2fs", job.getJobTime()));
                }else {
                    LOG.error("作业提交失败！！！参数配置无效~~~");
                    for(String name : newConfig.keySet()){
                        LOG.info(String.format("%s: %s",name, newConfig.get(name)));
                    }
                    failedCount++;
                    continue;
                }
                LOG.info(String.format("第%s次搜索结果：", iterateCount));
                String endTime = String.valueOf(System.currentTimeMillis()).substring(0,10);

                //进行多维度性能评分，结果作为个体的适应度
                if(job.getJobMode().equals("stream")) {
                    fitness = getMetrics.getMetric(endTime, job);
                    jobSubmit.killJob(job);
                }
                else if(job.getJobMode().equals("batch"))
                    fitness = getMetrics.getMetric(startTime, endTime, job);
                else {
                    LOG.error("作业类型错误，请选择作业类型：batch/stream ~~~");
                    continue;
                }
                LOG.info(String.format("作业性能综合评分：%s", fitness));
                totalScore.add(fitness);

                //保留当前种群最优个体
                if(bestfitness < fitness) {
                    isEvolution = true;
                    bestfitness = fitness;
                    bestConf = newConfig;
                    bestStr = str;
                    Map<String, String> map = getMetrics.getQueryResult();
                    for (String key : map.keySet()) {
                        bestResult.put(key, map.get(key));
                    }
                }
            }catch (Exception e){
                LOG.error(e.toString());
                continue;
            }
            populationFitness[i] = fitness;
            allResult.put(str, fitness);
        }
        return populationFitness;
    }


    /**
     * 根据适应度计算结果，进行轮盘选择
     * @param evals（种群个体的适应度）
     */
    private void select(double[] evals) {
        double p[] = new double[ChrNum];
        double F = 0;
        // 累计适应值总和
        for (int i = 0; i < ChrNum; i++) {
            F = F + evals[i];
        }
        for (int i = 0; i < ChrNum; i++) {
            p[i] = evals[i] / F;  //计算个体被选择概率
        }
         //根据初始参数个数计算一个适应概率，淘汰低于概率的个体
        double r = 1.0/ChrNum;
        for (int i = 0; i < ChrNum; i++) {
            if(p[i] < r){
                if(i == 0){
                    ipop[i] = bestStr;
                }else {
                    ipop[i] = ipop[(int)Math.random()*(i-1)];
                }
            }
        }
        LOG.info(Arrays.toString(ipop));
    }

    private void exchange(int index1, int index2){
         String temp = ipop[index1];
         ipop[index1] = ipop[index2];
         ipop[index2] = temp;
    }

    /**
     * 交叉操作  随机选择两两染色体交叉
     */
    private void cross() {
        String temp1, temp2;
        Random random = new Random();
        for (int i = 0; i < ChrNum/2 *2; i+=2) {
            int rnd;
            //随机两两配对
            rnd = random.nextInt(ChrNum - i)+i;
            if(rnd != i)
                exchange(i , rnd);
            rnd = random.nextInt(ChrNum - i)+i;
            if(rnd != i+1)
                exchange(i + 1 , rnd);
            //开始交叉
            int pos = (int)(Math.random()*GENE)+1;     // 随机生成交叉点pos
            temp1 = ipop[i].substring(0, pos) + ipop[(i + 1) % ChrNum].substring(pos);
            temp2 = ipop[(i + 1) % ChrNum].substring(0, pos) + ipop[i].substring(pos);
            if(rule(temp1) && rule(temp2)) { //符合规范才算交叉成功
                ipop[i] = temp1;
                ipop[(i + 1) / ChrNum] = temp2;
            }
        }
    }

    /**
     * 基因突变操作
     */
    private void mutation() {
        for (int i = 0; i < ChrNum+1; i++) {
            int num = (int) (Math.random() * GENE * ChrNum + 1);
            //随机选择某条染色体上的某个基因点
            int chromosomeNum = (int) (num / GENE) + 1; // 染色体号
            int mutationNum = num - (chromosomeNum - 1) * GENE; // 基因号
            if (mutationNum == 0)
                mutationNum = 1;
            chromosomeNum = chromosomeNum - 1;
            if (chromosomeNum >= ChrNum)
                chromosomeNum = ChrNum-1;
            String temp;
            String a = "0";   //记录变异位点变异后的编码，如果变异点本来为0，则变成1
            LOG.info(String.format("变异个体%s，变异位置%s", chromosomeNum, mutationNum));
            if (ipop[chromosomeNum].charAt(mutationNum - 1) == '0') {    //当变异位点为0时
                a = "1";
            }
            //当变异位点在首、中段和尾时的突变情况
            if (mutationNum == 1) {
                temp = a + ipop[chromosomeNum].substring(mutationNum);
            } else {
                if (mutationNum != GENE) {
                    temp = ipop[chromosomeNum].substring(0, mutationNum -1) + a
                            + ipop[chromosomeNum].substring(mutationNum);
                } else {
                    temp = ipop[chromosomeNum].substring(0, mutationNum - 1) + a;
                }
            }
            //变异后的染色体符合规范，记录下变异后的染色体
            if(rule(temp)) {
                ipop[chromosomeNum] = temp;
            }else {
                LOG.error("交叉变异个体不符合参数约束规则");
            }
        }
    }

    /**
     * 最优参数搜索的迭代过程
     * @param target（性能目标）
     */
    public  void iterateSearch(double target) {

        //记录迭代轮数
        int rounds = 0;
        int notEvoluteCount = 0;
        int degenerateMax = Config.getInt("max.degenerate.count");
        int roundsMax =  Config.getInt("genetic.iteration.max");
        Long startSearch  = System.currentTimeMillis();

        //当作业评分达到指定目标 或者 连续三次迭代没有进化初更好的个体
        while(bestfitness < target && notEvoluteCount <  degenerateMax){
            if(rounds > roundsMax) break;
            LOG.info(String.format("种群%d编码: %s", rounds, Arrays.toString(ipop)));
            //筛选优秀的父母
            select(calculateFitness());
            //交叉与变异
            cross();
            mutation();
            //每次进化最优秀个体
            if(isEvolution) {
                LOG.info(String.format("种群%d中最优个体性能评分：%s", rounds, bestfitness));
                LOG.info("参数配置：");
                for (String name : bestConf.keySet()) {
                    LOG.info(String.format("%s: %s", name, bestConf.get(name)));
                }
                LOG.info("性能指标：");
                for (String key : bestResult.keySet()) {
                    LOG.info(String.format("%s: %s", key, bestResult.get(key)));
                }
                notEvoluteCount = 0;
            }else {
                LOG.warn(String.format("种群%d没有进化出更优秀个体~~~", rounds));
                notEvoluteCount++;
            }
            jobScore.add(bestfitness);
            rounds++;
            isEvolution = false;
        }
        Long endSearch = System.currentTimeMillis();
        String jobMode = Config.getString("flink.job.mode");

        //输出系统运行结果
        System.out.println("==========参数搜索结束==========");
        if(bestfitness < target){
            System.out.println("未达到性能目标....");
        }
        System.out.println(String.format("不加优化参数搜索次数：%s，实际参数搜索次数: %s, 作业执行成功次数：%s，作业执行失败次数：%s",
                rounds*ChrNum - failedCount, iterateCount, iterateCount - failedCount, failedCount));

        System.out.println("推荐参数配置：");
        for(String name : bestConf.keySet()){
            System.out.println(String.format("%s: %s",name, bestConf.get(name)));
        }
        System.out.println("推荐参数的性能指标：");
        PrintResult printResult = new PrintResult();
        printResult.printMetrics(bestResult);
        System.out.println("推荐配置的性能评分："+ bestfitness);
        System.out.println("总执行时间："+ (endSearch-startSearch)/(1000*60) + "分钟") ;

        //打印在Log文件里的结果，更详细
        LOG.info("==========参数搜索结束==========");
        if(bestfitness < target){
            LOG.info("未达到性能目标....");
        }
        LOG.info(String.format("不加优化参数搜索次数：%s，实际参数搜索次数: %s, 作业执行成功次数：%s，作业执行失败次数：%s",
                rounds*ChrNum - failedCount, iterateCount, iterateCount - failedCount, failedCount));

        LOG.info("推荐参数配置：");
        for(String name : bestConf.keySet()){
            LOG.info(String.format("%s: %s",name, bestConf.get(name)));
            printResult.writeTrainData(bestResult);
        }
        LOG.info("推荐参数的性能指标：");
        for(String key  : bestResult.keySet()){
            LOG.info(String.format("%s: %s",key, bestResult.get(key)));
        }
        LOG.info("推荐配置的性能评分："+ bestfitness);
        if(jobMode.equals("batch"))
            LOG.info("搜索过程中作业执行时间变化：" + jobTimes);
        else {
            LOG.info("搜索过程中作业延迟时间变化：" + getMetrics.jobLatency);
            LOG.info("搜索过程中作业消费速率变化：" + getMetrics.jobConsumption);
        }
        LOG.info("种群迭代过程中作业性能评分变化：" + jobScore);
        LOG.info("搜索过程中作业能评分变化：" + totalScore);
        LOG.info("总执行时间："+ (endSearch-startSearch)/(1000*60) + "分钟");

    }

}