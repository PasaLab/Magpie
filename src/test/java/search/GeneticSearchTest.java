package search;

import util.Config;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;

public class GeneticSearchTest extends Config {



    @Test
    public void getGeneNum() {
        Map<String, String[]> paras = new LinkedHashMap<>();
        String[] values = {"4g","20g"};
        paras.put("taskmanager.memory.flink.size", values);
        paras.put("taskmanager.numberOfTaskSlots", new String[]{"4","20"});
        paras.put("taskmanager.memory.network.fraction", new String[]{"0.05","0.25"});
        GeneticSearch geneticSearch = new GeneticSearch(paras);
        assertEquals(Integer.toBinaryString(20).length()*2+Integer.toBinaryString(25).length(),
                geneticSearch.GENE);
    }

    @Test
    public void getParasIndex() {
        Map<String, String[]> paras = new LinkedHashMap<>();
        String[] values = {"4g","20g"};
        paras.put("taskmanager.memory.flink.size", values);
        paras.put("taskmanager.numberOfTaskSlots", new String[]{"4","20"});
        paras.put("taskmanager.memory.network.fraction", new String[]{"0.05","0.25"});
        GeneticSearch geneticSearch = new GeneticSearch(paras);


    }

    @Test
    public void strToConfig(){
        Map<String, String[]> paras = new LinkedHashMap<>();
        String[] values = {"4g","8g","10g","16g","20g"};
        paras.put("taskmanager.memory.flink.size", values);
        paras.put("taskmanager.numberOfTaskSlots", new String[]{"4","6","8"});
        paras.put("taskmanager.memory.network.fraction", new String[]{"0.05","0.10","0.15","0.20"});
        GeneticSearch geneticSearch = new GeneticSearch(paras);
        Map<String, String> config = new LinkedHashMap<>();
        config.put("taskmanager.memory.flink.size", "7g");
        config.put("taskmanager.numberOfTaskSlots", "10");
        config.put("taskmanager.memory.network.fraction", "0.15");
        String str = geneticSearch.configToStr(config);
        assertEquals(config, geneticSearch.strToConfig(str));


    }

    @Test
    public void configToStr(){
        Map<String, String[]> paras = new LinkedHashMap<>();
        String[] values = {"4g","8g","10g","16g","20g"};
        paras.put("taskmanager.memory.flink.size", values);
        paras.put("taskmanager.numberOfTaskSlots", new String[]{"4","6","8"});
        paras.put("taskmanager.memory.network.fraction", new String[]{"0.05","0.10","0.15","0.20"});
        GeneticSearch geneticSearch = new GeneticSearch(paras);
        Map<String, String> config = new LinkedHashMap<>();
        config.put("taskmanager.memory.flink.size", "7g");
        config.put("taskmanager.numberOfTaskSlots", "10");
        config.put("taskmanager.memory.network.fraction", "0.15");
        System.out.println(geneticSearch.configToStr(config));
        assertEquals(geneticSearch.GENE, geneticSearch.configToStr(config).length());



    }

    @Test
    public void iterateSearch() {
//        Map<String, String[]> paras = new LinkedHashMap<>();
//        String[] values = {"4g","8g","10g","16g","20g"};
//        paras.put("taskmanager.memory.flink.size", values);
//        paras.put("taskmanager.numberOfTaskSlots", new String[]{"4","6","8"});
//        paras.put("taskmanager.memory.network.fraction", new String[]{"0.05","0.1","0.15","0.2"});
//        GeneticSearch geneticSearch = new GeneticSearch(paras);
//        geneticSearch.iterateSearch(0.9);
    }
}