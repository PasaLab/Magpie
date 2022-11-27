package analysis;

import util.Config;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class ConfigTest extends Config {

    @Test
    public void testGetMap() {
        Map<String, Object> map = Config.getMap("parameters");
        for(String para : map.keySet()){
            assertNotNull(map.get(para));

        }
    }

    @Test
    public void testGetConfig() {
        System.out.println(Config.getConfig());
    }
}