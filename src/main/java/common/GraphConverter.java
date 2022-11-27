package common;

import graph.Edge;
import graph.Graph;
import graph.Node;
import graph.label;
import util.HdfsClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.HashMap;


public class GraphConverter {

    /**
     * 将json结构的图转化成类Graph
     * @param graph
     * @return
     */
    public static Graph jsonToDAG(JSONArray graph){
        HashMap<Integer, Node> nodes = new HashMap<>();
        Graph dg = new Graph() ;
        for (Object g : graph) {
            if (g instanceof JSONObject) {
                JSONObject json = JSONObject.parseObject(g.toString());
                int id = json.getInteger("id");
                //点转换
                Node node = new Node(id, new label(json.getString("pact"),
                        json.getInteger("parallelism"), json.getString("contents")));
                nodes.put(id, node);
                dg.addNode(node);
                //边转换
                if (json.containsKey("predecessors")) {
                    JSONArray pre = JSONArray.parseArray(json.get("predecessors").toString());
                    for (Object p : pre) {
                        JSONObject pNode = JSONObject.parseObject(p.toString());
                        int from = pNode.getInteger("id");
                        dg.addEdge(new Edge(nodes.get(from), nodes.get(id), pNode.getString("ship_strategy")));
                    }
                }
            }
        }
        return dg ;
    }

    /**
     * 读取边和节点，转化为graph
     */
    public static Graph toGraph(Path nodePath, Path edgePath) {
        Graph graph = new Graph();
        try {
            FileSystem fs = HdfsClient.getFileSystem();
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(nodePath)));
            String line;
            while ((line = reader.readLine()) != null) {
                graph.addNode(new Node(Integer.parseInt(line)));
            }
            reader.close();
            reader = new BufferedReader(new InputStreamReader(fs.open(edgePath)));
            while ((line = reader.readLine()) != null) {
                int form = Integer.parseInt(line.split(" ")[0]);
                int to = Integer.parseInt(line.split(" ")[1]);
                graph.addEdge(new Edge(new Node(form), new Node(to)));
            }
            reader.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return graph;
    }
}
