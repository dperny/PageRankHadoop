import java.util.*;

public class Node {
  /*
   * Node represented in text as:
   * ID   \t  EDGES|RANK
   * 0      1,2,5,6|125
   */
  public final int id;
  public List<Integer> edges = new ArrayList<Integer>();
  public int rank = 0;

  /**
   * Create a new node from its string representation
   *
   * @param str the string representation of the node
   */
  public Node(String str) {
    // split the string into the id and data
    String[] split = str.split("\t");
    String key = split[0];
    String data = split[1];

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

  public Node(int id) {
    this.id = id;
  }

  /**
   * Return the full string representation of the node
   */
  public String toString() {
    return "" + id + "\t" + getDataString();
  }

  /**
   * Return the string representation of the node data
   */
  public String getDataString() {
    StringBuffer s = new StringBuffer();

    // add the adjacency list
    for (int v : edges) {
      s.append(v).append(",");
    }

    // divider
    s.append("|");

    // add the rank
    s.append(rank);

    return s.toString();
  }
}
