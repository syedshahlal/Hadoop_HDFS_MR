import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
         extends Mapper<Object, Text, Text, IntWritable> {
  
      static enum CountersEnum { INPUT_WORDS }
  
      private final static IntWritable one = new IntWritable(1);
      private Text word = new Text();
  
      private boolean caseSensitive;
      private Set<String> patternsToSkip = new HashSet<String>();
  
      private Configuration conf;
      private BufferedReader fis;
  
      @Override
      public void setup(Context context) throws IOException,
          InterruptedException {
        conf = context.getConfiguration();
        caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
        if (conf.getBoolean("wordcount.skip.patterns", false)) {
          URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
          for (URI patternsURI : patternsURIs) {
            Path patternsPath = new Path(patternsURI.getPath());
            String patternsFileName = patternsPath.getName().toString();
            parseSkipFile(patternsFileName);
          }
        }
      }
  
      private void parseSkipFile(String fileName) {
        try {
          fis = new BufferedReader(new FileReader(fileName));
          String pattern = null;
          while ((pattern = fis.readLine()) != null) {
            patternsToSkip.add(pattern);
          }
        } catch (IOException ioe) {
          System.err.println("Caught exception while parsing the cached file '"
              + StringUtils.stringifyException(ioe));
        }
      }
  
      @Override
      public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
        String line = (caseSensitive) ?
            value.toString() : value.toString().toLowerCase();
        for (String pattern : patternsToSkip) {
          line = line.replaceAll(pattern, "");
        }
  
        // Replace all non-word characters and numbers with spaces
        line = line.replaceAll("[^a-zA-Z ]", "");
  
        StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, one);
          Counter counter = context.getCounter(CountersEnum.class.getName(),
              CountersEnum.INPUT_WORDS.toString());
          counter.increment(1);
        }
      }
    }



  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
