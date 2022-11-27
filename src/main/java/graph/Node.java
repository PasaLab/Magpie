package graph;



public class Node {

    public int id;

    public label label;

    public Node(int id, label label) {
        this.id = id;
        this.label = label;
    }

    public Node (int id){
        this.id = id;
        this.label = null;
    }

    public label  getLabel() {
        return label;
    }

    public int getId() { return id; }

    @Override
    public String toString() {
        return String.valueOf(id);
    }

    @Override
    public  boolean equals(Object obj) {
        if (getClass() != obj.getClass()) return false ;
        Node other = (Node) obj;
        return label.equals(other.getLabel()) && id == other.getId();

    }
    @Override
    public int hashCode() {
        int res = id;
        res = 17*res + label.hashCode();
        return res;
    }
}