import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

  public static class MapClass extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, 
        OutputCollector<IntWritable, Text> output, Reporter reporter)
        throws IOException {

      Node node = new Node(value.toString());

      // the p-value is the rank of the current node divided by the # of nodes
      int p = node.rank / node.edges.size();

      // emit pagerank values as !p so that we can identify which values are 
      // nodes and which are p-values in the reduce step
      for(int edge : node.edges) { 
        output.collect(new IntWritable(edge), new Text("!" + p));
      }

      // output the node ID and the node's text representation
      output.collect(new IntWritable(node.id), new Text(node.toString()));
    }
  }

  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter) 
        throws IOException {

      int rank = 0;
      
      Node n = null;

      while(values.hasNext()) {
        Text value = values.next();

        // check if the value is a p-value
        if(value.toString().startsWith("!")) {
          // parse the int out of the remaining characters and add it to 
          rank += Integer.parseInt(value.toString().substring(1));
        } else {
          // otherwise, the value is the node itself
          n = new Node(value.toString());
        }
      }

      // update the node rank
      n.rank = rank;
      // and emit the node
      output.collect(key, new Text(n.toString()));
    }
  }

  static int printUsage() {
    System.out.println("pagerank [-m <num mappers>] [-r <num reducers>]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

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

  /**
   * The main driver for word count map/reduce program. Invoke this method to
   * submit the map/reduce job.
   * 
   * @throws IOException
   *           When there is communication problems with the job tracker.
   */
  public int run(String[] args) throws Exception {

    int iterationCount = 0;

    while (keepGoing(iterationCount)) {

      String input;
      if (iterationCount == 0)
        input = "input-graph";
      else
        input = "output-graph-" + iterationCount;

      String output = "output-graph-" + (iterationCount + 1);

      JobConf conf = getJobConf(args);
      FileInputFormat.setInputPaths(conf, new Path(input));
      FileOutputFormat.setOutputPath(conf, new Path(output));
      RunningJob job = JobClient.runJob(conf);

      iterationCount++;
    }

    return 0;
  }
  
  private boolean keepGoing(int iterationCount) {
    if(iterationCount >= 4) {
      return false;
    }
    
    return true;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PageRank(), args);
    System.exit(res);
  }
}
