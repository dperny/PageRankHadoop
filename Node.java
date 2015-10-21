import org.apache.hadoop.io.Text;

public class Node {
  /*
   * Node represented in text as:
   * ID   \t  EDGES|RANK
   * 0      1,2,5,6|125
   */
  private final int id;
  private List<Integer> edges;
  private int rank;

  public Node(String str) {
    // split the string into the id and data
    String[] split = str.split("\t");
    String key = split[0];
    String data = split[1]

    // parse the key and make the id it
    this.id = Integer.parseInt(key);

    // parse the adjacency list (token 0)
    String[] tokens = data.split("\\|");
    for (String s : tokens[0].split(",")) {
      if(s.length() > 0) {
        this.edges.add(Integer.parseInt(s));
      }
    }

    // parse the rank (token 1)
    this.rank = Integer.parseInt(tokens[1]);
  }

  public Text getTextRepr() {
    StringBuffer s = new StringBuffer();

    for (int v : edges) {

    }
  }
}
