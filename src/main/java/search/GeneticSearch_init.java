package search;

import analysis.FlinkJob;
import analysis.GetMetrics;
import analysis.JobExecute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prediction.LGBMPredict;
import util.Config;
import util.PrintResult;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GeneticSearch_init {
    private static final Logger LOG = LoggerFactory.getLogger(GeneticSearch_init.class);

    private int ChrNum ;	//染色体数量
    private String[] ipop;	 	//一个种群中染色体总数
    public  final int GENE ; 		//基因数
    private double bestfitness = Integer.MIN_VALUE;  //个体的适应值=作业性能综合评分
    public Map<String, String> bestResult = new LinkedHashMap<>();
   // public Double bestScore = 0d;
    private  String bestStr;  //当前最优染色体
    private Map<String, String> bestConf;  //最优解的染色体的参数配置
    private Map<String, String[]> paras;  //参数名和参数值
    private Map<String, Integer> parasIndex; //参数在染色体上的位置
    private Boolean isEvolution = false;
    private GetMetrics getMetrics;
    private int failedCount = 0;
    private Map<String, Double> allResult = new LinkedHashMap<>();
    private int iterateCount = 0;
    private ArrayList<String> jobTimes = new ArrayList<>();
    private ArrayList<Double> jobScore = new ArrayList<>();
    private ArrayList<Double> totalScore = new ArrayList<>();

    public GeneticSearch_init(Map<String, String[]> paras){
        this.paras = paras;
        this.parasIndex = getParasIndex(paras);
        this.GENE = getGeneNum(paras);
        this.ChrNum = paras.size()+1;
        this.ipop = new String[paras.size()];	 	//一个种群大小=参数个数
        this.getMetrics =  new GetMetrics();
    }


    public int getGeneNum(Map<String, String[]> paras){
        int geneNum = 0;
        for(String para : paras.keySet()){
            geneNum += Integer.toBinaryString(paras.get(para).length-1).length();
        }
        return geneNum;
    }

    /**
     * 记录参数再染色体上的位置
     * @param paras
     * @return
     */
    public Map<String, Integer> getParasIndex(Map<String, String[]> paras){
        int geneIndex = 0;
        Map<String, Integer> parasIndex = new LinkedHashMap<>();
        for(String para : paras.keySet()){
            geneIndex += Integer.toBinaryString(paras.get(para).length-1).length();
            parasIndex.put(para, geneIndex);
        }
        return parasIndex;
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


    public static Double getNum(String str){
        return Double.parseDouble(str.replaceAll("[^0-9]", ""));
    }

    /**
     * 判断染色体是否在参数取值内
     */
    public boolean rule(String str){
        int start = 0;
        List<String> confs = new ArrayList<>();
        for(String para : paras.keySet()){
            int index = Integer.parseInt(str.substring(start, parasIndex.get(para)), 2);
            start = parasIndex.get(para);
            if (index >= paras.get(para).length)
                return false;
            confs.add(paras.get(para)[index]);
        }
        return  true;
    }

    /**
     * 初始化一条染色体（用二进制字符串表示）
     */
    private String initChr() {
        String res = "";
        for (int i = 0; i < GENE; i++) {
            if (Math.random() > 0.5) {
                res += "0";
            } else {
                res += "1";
            }
        }
        return res;
    }

    /**
     * 初始化一个种群
     */
    private String[] initPop() {
        String[] ipop = new String[ChrNum];
        for (int i = 0; i < ChrNum; i++) {
            String temp = initChr();
            while(!rule(temp)){
                temp = initChr();
            }
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
        LOG.info(String.format("%s,%s,%s", Arrays.toString(nums), target, nums[i-1]));
        if(i < 0 ) return i;
        if(i > nums.length) return nums.length-1;
        return i-1;
    }

    /**
     * 将染色体转换成一组参数配置
     * @param str
     * @return
     */
    private Map<String, String> strToConfig(String str){
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
     * 将染色体转换成参数的值
     * 作业运行得到性能反馈
     */
    private double[] calculateFitness( ) {
        double[] populationFitness = new double[ChrNum];
        //记录当前种群中最优个体
        Double best = 0d;
        for(int i = 0; i < ChrNum; i++){
            String str = ipop[i];
            double fitness = 0d;
            Map<String, String> newConfig = strToConfig(str);
            iterateCount++;
            try {
                //bug
                //String startTime = String.valueOf(System.currentTimeMillis()-16*60*60*1000).substring(0,10);
                String startTime = String.valueOf(System.currentTimeMillis()).substring(0,10);
                JobExecute jobSubmit = new JobExecute();
                //run task
                //把参数作为命令行  提交job
                FlinkJob job = jobSubmit.executeJob(newConfig);
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
                //bug
                //String endTime = String.valueOf(System.currentTimeMillis()-16*60*60*1000).substring(0,10);
                String endTime = String.valueOf(System.currentTimeMillis()).substring(0,10);
               // fitness = getMetrics.getMetric(startTime, endTime, job);

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
                jobScore.add(fitness > bestfitness ? fitness : bestfitness);
                totalScore.add(fitness);
                //选当前种群最好的那个
               // best = fitness > best ? fitness : best;
                //LOG.info("作业执行时间："+jobTimes.get(iterateCount-1));
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
        //totalScore.add(best);
        return populationFitness;
    }

    /**
     * 轮盘选择
     * 计算群体上每个个体的适应度值;
     * 按由个体适应度值所决定的某个规则选择将进入下一代的个体;
     */
    private void select(double[] evals) {
        //double evals[] = new double[ChrNum]; // 所有染色体适应值
        double p[] = new double[ChrNum]; // 各染色体选择概率
        double q[] = new double[ChrNum]; // 累计概率
        double F = 0; // 累计适应值总和
        for (int i = 0; i < ChrNum; i++) {
            F = F + evals[i]; // 所有染色体适应值总和
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
            }
        }
    }

    public  void iterateSearch(double target) {
        //产生初始种群
        ipop = initPop();
        //记录迭代轮数
        int rounds = 0;
        int notEvoluteCount = 0;
        int degenerateMax = Config.getInt("max.degenerate.count");
        int roundsMax =  Config.getInt("genetic.iteration.max");
        Long startSearch  = System.currentTimeMillis();
        //当作业评分达到指定目标 或者 连续三次迭代没有进化初更好的个体
        while(bestfitness < target && notEvoluteCount < degenerateMax){
            if(rounds > roundsMax) break;
            LOG.info(String.format("种群%d编码: %s", rounds, Arrays.toString(ipop)));
            //筛选优秀的父母
            select(calculateFitness());
            //优秀个体染色体交叉
            cross();
            //基因突变
            mutation();
            //打印每次进化最优秀个体
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
        System.out.println(String.format("实际参数搜索次数: %s, 作业执行成功次数：%s，作业执行失败次数：%s",
                iterateCount, iterateCount - failedCount, failedCount));

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
