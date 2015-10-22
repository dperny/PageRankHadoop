import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

  public static class MapClass 
      extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

      Node node = new Node(value.toString());
      System.out.println("Key: " + key + " Value: " + value.toString());

      // the p-value is the rank of the current node divided by the # of nodes
      int p = node.rank / node.edges.size();

      System.out.println("Node " + node.id + " rank = " + node.rank);
      System.out.println("Node " + node.id + " p = " + p);

      // emit pagerank values as !p so that we can identify which values are 
      // nodes and which are p-values in the reduce step
      for(int edge : node.edges) { 
        context.write(new IntWritable(edge), new Text("!" + p));
      }

      // output the node ID and the node's text representation
      context.write(new IntWritable(node.id), new Text(node.getDataString()));
    }
  }

  public static class Reduce 
      extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
        throws IOException, InterruptedException {

      // holds the rank this node will recieve
      int rank = 0;
      
      // a place for our node, when we find it.
      Node n = null;

      for(Text value : values) {
        // check if the value is a p-value
        if(value.toString().startsWith("!")) {
          // parse the int out of the remaining characters and add it to 
          rank += Integer.parseInt(value.toString().substring(1));
        } else {
          // otherwise, the value is the node itself
          System.out.println(value.toString());
          n = new Node(key + "\t" + value.toString());
        }
      }

      // update the node rank
      n.rank = rank;
      // and emit the node
      System.out.println(n.toString());
      context.write(key, new Text(n.getDataString()));
    }
  }

  static int printUsage() {
    System.out.println("pagerank [-m <num mappers>] [-r <num reducers>]");
    // ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  /*
  private JobConf getJobConf(String[] args) {
    JobConf conf = new JobConf(getConf(), PageRank.class);
    conf.setJobName("pagerank");

    // the keys are the unique identifiers for a Node (ints in this case).
    conf.setOutputKeyClass(IntWritable.class);
    // the values are the string representation of a Node
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(MapClass.class);
    conf.setReducerClass(Reduce.class);

    for (int i = 0; i < args.length; ++i) {
      if ("-m".equals(args[i])) {
        conf.setNumMapTasks(Integer.parseInt(args[++i]));
      } else if ("-r".equals(args[i])) {
        conf.setNumReduceTasks(Integer.parseInt(args[++i]));
      }
    }

    return conf;
  }
  */

  public static int getIterations(String filename) throws Exception {
    // read the second line to get the number of iterations
    BufferedReader br = new BufferedReader(new FileReader(filename));
    // skip the first line, contains nothing of value
    br.readLine();
    return Integer.parseInt(br.readLine().trim());
  }

  public static List<Node> getNodes(String filename) throws Exception {
    // open the file
    BufferedReader br = new BufferedReader(new FileReader(filename));
    // get the number of nodes
    int count = Integer.parseInt(br.readLine().split(" ")[0]);
    // skip the second line. it contain nothing of value
    br.readLine();

    // populate the node list
    List<Node> nodes = new ArrayList<Node>();
    for(int i = 0; i < count; i++) {
      Node n = new Node(i + 1);
      n.rank = 1000 / count;
      nodes.add(n); 
    }

    String line = br.readLine();
    while(line != null) {
      String[] pair = line.trim().split(" ");

      // add the second node to the adjacency list of the second node
      nodes.get(Integer.parseInt(pair[0].substring(1)) - 1)
        .edges.add(Integer.parseInt(pair[1].substring(1)));

      line = br.readLine();
    }

    return nodes;
  }

  public static void writeInitialToHDFS(List<Node> nodes) throws Exception {
    Configuration config = new Configuration();
    FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), config);
    Path file = new Path("hdfs://localhost:54310/user/hduser/input");
    if(hdfs.exists(file)) {
      hdfs.delete(file, true);
    }
    OutputStream os = hdfs.create(file);
    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
    for(Node node : nodes) {
      bw.write(node.toString());
      bw.newLine();
    }

    bw.close();
    hdfs.close();
  }

  public static void main(String[] args) throws Exception {

    String file = "/home/hduser/graph.txt";

    int iterations = getIterations(file);
    List<Node> nodes = getNodes(file);

    writeInitialToHDFS(nodes);

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    Path in = new Path("/user/hduser/input");
    Path out = new Path("/user/hduser/output");

    Job job = new Job(conf, "pagerank");
    job.setJarByClass(PageRank.class);
    job.setMapperClass(MapClass.class);
    // job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, in);
    FileOutputFormat.setOutputPath(job, out);

    job.waitForCompletion(true);

    for(int i = 1; i < iterations; i++) {
      fs.delete(in);
      fs.rename(out, in);
      job = new Job(conf, "pagerank");
      job.setJarByClass(PageRank.class);
      job.setMapperClass(MapClass.class);
      // job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, in);
      FileOutputFormat.setOutputPath(job, out);
      job.waitForCompletion(true);
    }
    
    System.exit(0);
  }
}
