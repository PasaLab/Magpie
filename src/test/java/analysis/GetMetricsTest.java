package analysis;

import util.Config;
import org.junit.Test;

import java.util.ArrayList;

public class GetMetricsTest extends Config {
    GetMetrics getMetrics = new GetMetrics();

    @Test
    public void queryPrometheus() {
    }

    @Test
    public void getAvg() {
    }

    @Test
    public void getSourceVertices() {
    }

    @Test
    public void getMetric() throws Exception{
        String start = String.valueOf(System.currentTimeMillis()).substring(0,10);
        ArrayList<String> taskID = new ArrayList<>();
        taskID.add("container_1595748893553_1228_01_000002");
        FlinkJob job = new FlinkJob("application_1595748893553_1228", "", taskID, "", 100.2);
        Double score = getMetrics.getMetric(start, job);
        System.out.println(score);
    }
}