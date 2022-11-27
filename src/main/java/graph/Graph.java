package graph;

import org.apache.flink.table.planner.expressions.E;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prediction.DataSetFeature;

import java.util.*;

public class Graph {
    private static final Logger LOG = LoggerFactory.getLogger(Graph.class);

    protected int id;
    protected List<Node> nodes;
    protected HashMap<Integer, List<Edge>> edges;

    public Graph() {
        nodes = new ArrayList<>();
        edges = new HashMap<>();
    }

    public void addNode(Node node) {
        if(!edges.containsKey(node.getId())) {
            edges.put(node.getId(), new ArrayList<>());
        }
        nodes.add(node);
    }


    public HashMap<Integer, List<Edge>> getEdges() {
        return edges;
    }

    public void addEdge(Edge edge) {
        edges.get(edge.getFrom().getId()).add(edge);
    }
    //根据节点或者节点ID 获取 节点所有的输出边
    public List<Edge> getEdges(int nodeId) {
        return edges.get(nodeId);
    }

    public List<Edge> getEdges(Node node) {
        return getEdges(node.getId());
    }



    Node getNode(int id) {
        for (Node node : nodes){
            if (node.getId() == id)
                return node;
        }
        return null;
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public int getId() {
        return id ;
    }

    /*
    图特征：节点数
     */
    public int getNodeNum() {
        return nodes.size();
    }

    /*
    图特征： 边数
     */
    public int getEdgeNum() {
        int sum = 0;
        for (int id : edges.keySet()){
            sum += edges.get(id).size();
        }
        return sum;
    }

    /*
    图特征: 总度数（入度 =出度 = 边数）
     */
    public Double avgDegree(){
        Double in = 0d;
        for(int id : edges.keySet()){
            in += edges.get(id).size();
        }
        return in/getNodeNum();
    }

    /*
    图特征：路径数
    有向无环图
     */
   // boolean[][] visited = new boolean[nodes.size()+1][nodes.size()+1];
    public  int gePathNum(){
        int sum  = 0;
        for(Node node : startNode()){
            sum += getNum(node);
        }
        return sum;
    }

    private List<Node> startNode(){
        int[] indegree = new int[nodes.size()*2];
        for(int node : edges.keySet()){
            for(Edge edge : edges.get(node)){
                indegree[edge.to.getId()]++;
            }
        }
        List<Node> start = new ArrayList<>();
        for(int i = 0; i < nodes.size(); i++){
            if(indegree[nodes.get(i).getId()] == 0) {
                start.add(nodes.get(i));
            }
        }
        return start;
    }

    private int getNum(Node node){
        List<Edge> edges = getEdges(node);
        if(edges == null || edges.size() == 0)
            return 1;
        int sum = 0;
        for(Edge edge : edges){
            sum += getNum(edge.to);
        }
        return sum;
    }

    /*
    有向无环图 深度=最大路径长度
     */
    public int maxDepth(){
        int depth = 0;
        for(Node node : startNode()){
            int temp = getDepth(node, 0);
            depth = depth > temp? depth : temp;
        }
        return depth;
    }

    private int getDepth(Node start, int depth){
        List<Edge> edges = getEdges(start);
        if(edges == null || edges.size() == 0)
            return depth+1;
        if(start == null)
            return  depth;
        int max = depth;
        for(Edge edge : edges){
            int temp = getDepth(edge.to, depth++);
            max = max > temp ? max : temp;
        }
        return depth + max;
    }


    /*
    最短路径
     */
    public int minPath(){
        int depth = Integer.MAX_VALUE;
        for(Node node : startNode()){
            int temp = getPath(node, 0);
            depth = depth < temp? depth : temp;
        }
        return depth;
    }

    private int getPath(Node start, int depth){
        List<Edge> edges = getEdges(start);
        if(edges == null || edges.size() == 0)
            return depth+1;
        if(start == null)
            return  depth;
        int min = Integer.MAX_VALUE;
        for(Edge edge : edges){
            int temp = getPath(edge.to, depth++);
            min = min < temp ? min : temp;
        }
        return depth + min;
    }


    /**
     * DFS遍历 统计图环数
     */
    private int[] visited;//节点状态,值为0的是未访问的
    private ArrayList<Integer> trace=new ArrayList<Integer>();//从出发节点到当前节点的轨迹
    private int cycleNum = 0;

    private void findCycle(int v){
        if(visited[v]==1) {
            if((trace.indexOf(v))!=-1)
            {
                System.out.println(("环："+trace));
                System.out.println("visited:"+Arrays.toString(visited));
                cycleNum++;
                return;
            }
            return;
        }
        visited[v]=1;
        trace.add(v);

        for(Edge edge : edges.get(v)) {
            findCycle(edge.to.id);
        }
        trace.remove(trace.size()-1);
    }


    public int getCycleNum() {
        visited=new int[nodes.size()+1];
        Arrays.fill(visited,0);
        findCycle(0);
        return cycleNum;
    }


//    public static void main(String[] args) {
//        Graph graph = new Graph();
//        graph.addNode(new Node(0));
//        graph.addNode(new Node(1));
//        graph.addNode(new Node(2));
//        graph.addNode(new Node(3));
//        graph.addNode(new Node(4));
//        graph.addNode(new Node(5));
//        graph.addEdge(new Edge(graph.getNode(0),graph.getNode(1) ));
//        graph.addEdge(new Edge(graph.getNode(1),graph.getNode(2) ));
//        graph.addEdge(new Edge(graph.getNode(2),graph.getNode(1) ));
//        graph.addEdge(new Edge(graph.getNode(2),graph.getNode(3) ));
//        graph.addEdge(new Edge(graph.getNode(4),graph.getNode(3) ));
//        graph.addEdge(new Edge(graph.getNode(3),graph.getNode(4) ));
//        System.out.println(graph.getEdges(2));
//        System.out.println(graph.getCycleNum());
//    }

}