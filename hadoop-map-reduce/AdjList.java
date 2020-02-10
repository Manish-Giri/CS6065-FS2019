import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AdjList {
  private static int longestAdjListLength = -1;
  private static String longestAdjList = "";
  private static String nodeWithLongestAdjList = "";
  private static String nodeWithMaximumConnectivity = "";
  private static String nodeWithMinimumConnectivity = "";
  private static int nodeMinConn = 9999;
  private static int nodeMaxConn = -1;

  // variables for directed graph
  private static String longestAdjListDirected = "";
  private static int longestAdjListDirectedLength = -1;
  private static String nodeWithLongestAdjListDirected = "";
  private static int shortestAdjListDirectedLength = 99999;
  // directed - connectivity
  private static String nodeWithMaxConnDirected = "";
  private static String nodeWithMinConnDirected = "";
  private static int nodeMaxConnCountDirected = -1;
  private static int nodeMinConnCountDirected = -1;
  
 
  public static class AdjMapper
       extends Mapper<Object, Text, Text, Text>{

      //private final static IntWritable one = new IntWritable(1);
      private Text outVal = new Text();
      private Text outKey = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	String inline = value.toString();
		if (!inline.startsWith("#")){
			String[] inVals = inline.split("\t");
			outKey.set(inVals[0]);
			outVal.set(inVals[1]);


			// only directed graph -> key: key+d, value: v
			String dirKeyStr = inVals[0]+"d";
			Text dirKey = new Text();
			dirKey.set(dirKeyStr);
			context.write(dirKey, outVal);

			// undirected graph -> (key, value) + (value, key)	
			context.write(outKey, outVal);
			context.write(outVal, outKey);
		}
    }
  }

  public static class AdjReducer
      extends Reducer<Text,Text,Text, Text> {
    private Text result = new Text();
    
    private MultipleOutputs mos;
    private HashSet<String> set;
   // private HashSet<String> undirectedSet;
    
    public void setup(Context context) {
	  mos = new MultipleOutputs(context);
      set = new HashSet<>();
	  // no need of hashset in case of directed graph
	  // directedSet = new HashSet<>();
	  // undirectedSet = new HashSet<>();
    }
	
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

	
	String k = key.toString();
	int directedCtr = 0;
	String directedAdjList = "";
	
	// logic for directed graph - key ends with "d"
    if(k.endsWith("d")) {
	  // increment directed ctr, set directedadjlist string	
	  for(Text val: values) {
	     directedAdjList = directedAdjList + "," + val;
	     directedCtr++;
	  }
	  // remove starting ,
	  directedAdjList = directedAdjList.substring(1);

	  // if counter value is greater than current directedadjlistlength - set 
	  if(directedCtr > longestAdjListDirectedLength) {
		longestAdjListDirectedLength = directedCtr;
		longestAdjListDirected = directedAdjList;
                String maxNode = k.substring(0, k.length()-1);
	  	nodeWithLongestAdjListDirected = maxNode;
                nodeWithMaxConnDirected = maxNode;
		nodeMaxConnCountDirected = directedCtr;
	  }
	  // for minumun connectivity  - set
	  if(directedCtr < shortestAdjListDirectedLength ) {
		shortestAdjListDirectedLength = directedCtr;
		nodeWithMinConnDirected = k.substring(0, k.length()-1);
		nodeMinConnCountDirected = directedCtr;
	  }

    }	// end if	
      
	else {
	  // logic for undirected graph 	
	  int cntr = 0;
      String adjList = "";
 
        for (Text val : values) {
            // insert into hashset to remove dups
            // BUGFIX -> converting Text to String and then put into Set to match hash
            String curr = val.toString();
            set.add(curr);
        }
 
 
        for (String val : set) {
              adjList = adjList+","+val;
              cntr++;
        }
          
         adjList = adjList.substring(1);
 
 
         // longest adjacency list
         if(cntr > longestAdjListLength) {
           longestAdjListLength = cntr;
           longestAdjList = adjList;
           nodeWithLongestAdjList = key.toString();
           nodeMaxConn  = cntr;
         }
 
 
         // minimum connectivity
         if(cntr < nodeMinConn) {
           nodeMinConn = cntr;
           nodeWithMinimumConnectivity = key.toString();
         }

      
        adjList = adjList+"#"+cntr;
        result.set(adjList);
        context.write(key, result);

        //IMP - clear hashset, else new list is added to existing set
        set.clear();
	
       }
      
     }


    public void cleanup(Context context) throws IOException, InterruptedException {
      // WRITE -> DIRECTED graph 
      // node -> list
      mos.write("directedlargestadjlist", new Text(nodeWithLongestAdjListDirected), new Text(longestAdjListDirected));
      // nodewithmaxconnectivity, count
      mos.write("directedmaxconn", new Text(nodeWithMaxConnDirected), new Text(String.valueOf(nodeMaxConnCountDirected)));
      // nodewthminimumconnectivity, count
      mos.write("directedminconn", new Text(nodeWithMinConnDirected), new Text(String.valueOf(nodeMinConnCountDirected)));

      // WRITE -> UNDIRECTED GRAPH
      // write - node (with longest list) -> adj list
      mos.write("text", new Text(nodeWithLongestAdjList), new Text(longestAdjList));
      
      // write - node with maximum connectivity (node -> number)
      mos.write("undirectedmaxconn", new Text(nodeWithLongestAdjList), new Text(String.valueOf(nodeMaxConn)));
 
      // write - node with minimum connectivity (node -> number)
      mos.write("undirectedminconn", new Text(nodeWithMinimumConnectivity), new Text(String.valueOf(nodeMinConn)));

      mos.close();
          
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MapRed One Pass No Dups v2");
    job.setJarByClass(AdjList.class);
    job.setMapperClass(AdjMapper.class);
    job.setCombinerClass(AdjReducer.class);
    job.setReducerClass(AdjReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "undirectedmaxconn", TextOutputFormat.class, Text.class, Text.class);	
    MultipleOutputs.addNamedOutput(job, "undirectedminconn", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "directedlargestadjlist", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "directedmaxconn", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "directedminconn", TextOutputFormat.class, Text.class, Text.class);
    
    //MultipleOutputs.addNamedOutput(job, "fiveconn", TextOutputFormat.class, Text.class, Text.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
