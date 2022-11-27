package graph;


public class Edge {

   // protected String id;
    public Node from;
    public Node to;
    public String weight;

    public Edge(Node from, Node to, String weight) {
        this.from = from;
        this.to = to;
        this.weight = weight;
    }

    public Edge(Node from, Node to) {
        this.from = from;
        this.to = to;
        this.weight = null;
    }

    public Node getFrom() {
        return from;
    }

    public Node getTo() {
        return to;
    }

    @Override
    public String toString() {
        return from + "-" +to;
    }

    @Override
    public boolean equals(Object obj) {
        if(getClass() != obj.getClass()) return false ;
        Edge other = (Edge) obj;
        return  from.equals(other.from) && to.equals(other.to) && weight.equals(other.weight);
    }

    @Override
    public int hashCode() {
        return weight.hashCode();
    }
}