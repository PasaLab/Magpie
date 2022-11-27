package analysis;

import util.Config;

import java.util.ArrayList;

public class FlinkJob {
    private String applicationId;
    private String webUI;
    private ArrayList<String> taskID;
    private String jobStatus;
    private Double jobTime;
    private String jobMode;

    public FlinkJob(String applicationId, String webUI, ArrayList<String> taskID, String jobStatus, Double jobTime){
        this.applicationId = applicationId;
        this.webUI = webUI;
        this.taskID = taskID;
        this.jobStatus = jobStatus;
        this.jobTime = jobTime;
        this.jobMode = Config.getString("flink.job.mode");
    }


    public String getApplicationId(){
        return this.applicationId;
    }

    public String getWebUI(){
        return this.webUI;
    }

    public ArrayList<String> getTaskID(){
        return this.taskID;
    }

    public Double getJobTime(){
        return this.jobTime;
    }

    public String getJobStatus(){
        return this.jobStatus;
    }

    public String getJobMode(){
        return this.jobMode;
    }

    public void setTaskID(ArrayList<String> taskID){
        this.taskID  = taskID;
    }

    public void setJobStatus(String status){
        this.jobStatus = status;
    }

    public void setJobTime(Double time){
        this.jobTime = time;
    }

    public void setApplicationId(String applicationId){
        this.applicationId = applicationId;
    }

    public  void setWebUI(String webUI){
        this.webUI = webUI;
    }
}
