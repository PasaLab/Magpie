package graph;

public class label{
    private String pact;
    private int parallelism;
    private String content;

    public label(String pact, int parallelism, String content){
        this.pact = pact;
        this.content = content;
        this.parallelism = parallelism;
    }

    public String getPact(){ return pact; }

    public String getContent(){ return content; }

    public int getParallelism() { return parallelism; }

    @Override
    public  boolean equals(Object obj) {
        if (getClass() != obj.getClass()) return false ;
        label other = (label) obj;
        return content.equals(other.content);

    }
    @Override
    public int hashCode() {
        int res = pact.hashCode();
        res = 17*res + content.hashCode();
        return res;
    }

}