import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Do not change the signature of this class
public class TextAnalyzer extends Configured implements Tool {

    // Replace "?" with your own output key / value types
    // The four template data types are:
    //     <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
        	String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			HashMap<String, Integer> masterCount = new HashMap<String, Integer>();
			
			while (tokenizer.hasMoreTokens()) {
				String rawWord = tokenizer.nextToken();
				rawWord.toLowerCase();
				rawWord.replaceAll("[^a-zA-Z0-9]", " ");
				if(!masterCount.containsKey(rawWord)) {
					masterCount.put(rawWord, 1);
				} else {
					int current = masterCount.get(rawWord);
					masterCount.put(rawWord, current+1);
				}
			}
			
			//Loop this for the Map
			for(String rawWord : masterCount.keySet()) {
				HashMap<String, Integer> revisedCount = new HashMap<String, Integer>();
				revisedCount.putAll(masterCount);
				if(revisedCount.get(rawWord) == 1) {
					revisedCount.remove(rawWord);
				} else {
					int current = revisedCount.get(rawWord);
					revisedCount.put(rawWord, current-1);
				}
				Text word = new Text(rawWord);
				MapWritable map = new MapWritable();
				for(String n : masterCount.keySet()) {
					Text text = new Text(n);
					IntWritable num = new IntWritable(masterCount.get(n));
					map.put(text, num);
				}
				context.write(word, map);
			}
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
        public void reduce(Text key, Iterable<MapWritable> tuples, Context context)
            throws IOException, InterruptedException
        {
			MapWritable map = new MapWritable();
			MapWritable tempMap;
			Set<Writable> removeSet = new HashSet<Writable>();
			Iterator<MapWritable> iterator = tuples.iterator();
			if(iterator.hasNext()) {
				map = iterator.next();
			}
			while (iterator.hasNext()) {
				tempMap = iterator.next();
				for(Writable word : tempMap.keySet()) {
					if(tempMap.containsKey(word)) {
						IntWritable val = new IntWritable(((IntWritable) map.get(word)).get() + ((IntWritable) tempMap.get(word)).get());
						map.put(word, val);
						removeSet.remove(word);
					}
				}
				for(Writable word : removeSet) {
					tempMap.remove(word);
				}
				map.putAll(tempMap);
			}
			context.write(key, map);
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, MapWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<MapWritable> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you reducer function
            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out the current context key
	            context.write(key, emptyText);
	            //   Write out query words and their count
	            //Should only have one map per queryTuples
	            for(MapWritable map : queryTuples) {
		            for(Writable queryWord: map.keySet()){
		                String count = map.get(queryWord).toString() + ">";
		                Text queryWordText = new Text("<" + queryWord.toString() + ",");
		                context.write(queryWordText, new Text(count));
		            }
		            //   Empty line for ending the current context key	
	            }
	            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "EID1_EID2"); // Replace with your EIDs
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);
        //   Uncomment the following line if you want to use Combiner class
        job.setCombinerClass(TextCombiner.class);
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    // Do not modify the main method
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // You may define sub-classes here. Example:
    // public static class MyClass {
    //
    // }
}
