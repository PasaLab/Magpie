package analysis;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

public class JobExecuteTest {

    JobExecute jobSubmit = new JobExecute();
    @Test
    public void getSubmitCmd() {

        Map<String, String> paras = new LinkedHashMap<>();
        paras.put("taskmanager.memory.process.size", "8g");
        paras.put("taskmanager.numberOfTaskSlots", "10");
        paras.put("taskmanager.memory.network.fraction", "0.1");
        System.out.println(jobSubmit.getSubmitCmd(paras));
}
    @Test
    public void  getApplicationStatus() throws Exception{
        String url = "http://localhost:8088/ws/v1/cluster/apps/application_1595748893553_0609";
        JSONObject app = jobSubmit.getJson(url).getJSONObject("app");
        System.out.println(app.get("state"));
        System.out.println(app.getInteger("elapsedTime")/1000);
    }

}