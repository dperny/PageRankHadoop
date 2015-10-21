import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
        OutputCollector<IntWritable, Text> output, Reporter reporter) {

      LOG.info("Map executing for key [" 
          + key.toString() + "] and value [" + value.toString() + "]");

      Node node = new Node(value.toString());

      output.collect(new IntWritable(node.getId()), node.getLine());

    }
  }

  public static class Reduce extends MapReduceBase implements
      Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterator<Text> values,
        OutputCollector<IntWritable, Text> output, Reporter reporter) {
      LOG.info("Reduce executing for input key [" + key.toString() + "]");

      while (values.hasNext()) {
        Text value = values.next();

        Node u = new Node(key.get() + "\t" + value.toString());
      }

      Node n = new Node(key.get());
      LOG.info("Reduce outputting final key [" + key + "] and value [" 
          + n.getLine() + "]");
    }
  }

  static int printUsage() {
    System.out.println("graphsearch [-m <num mappers>] [-r <num reducers>]");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  private JobConf getJobConf(String[] args) {
    JobConf conf = new JobConf(getConf(), GraphSearch.class);
    conf.setJobName("graphsearch");

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

    LOG.info("The number of reduce tasks has been set to " + conf.getNumReduceTasks());
    LOG.info("The number of mapper tasks has been set to " + conf.getNumMapTasks());

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
    int res = ToolRunner.run(new Configuration(), new GraphSearch(), args);
    System.exit(res);
  }

}
