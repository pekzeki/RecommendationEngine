package tr.edu.sabanci.hadoop.tfidf;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordFrequenceInDocument Creates the index of the words in documents,
 * mapping each of them to their frequency.
 */
public class WordFrequenceInDocument extends Configured implements Tool {

    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "/user/admin/project/tfidf/output/1-word-freq";

    public static class WordFrequenceInDocMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         * Default word
         */
        private Text word = new Text();
        /**
         * Default single counter
         */
        private IntWritable singleCount = new IntWritable(1);

        public WordFrequenceInDocMapper() {
        }

        /**
         * @param key is the byte offset of the current line in the file;
         * @param value is the line from the file
         *
         *     POST-CONDITION: Output <"word", "offset"> pairs
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            // build the values and write <k,v> pairs through the context
            StringBuilder valueBuilder = new StringBuilder();
            String line[] = value.toString().split("\t");
            if(line.length > 1) {
                String[] words = line[1].split(",");
                for (String w : words) {
                    valueBuilder.append(line[0].trim());
                    valueBuilder.append("\t");
                    valueBuilder.append(w.trim());
                    this.word.set(valueBuilder.toString());
                    context.write(this.word, this.singleCount);
                    valueBuilder.setLength(0);
                }
            }
            // emit the partial <k,v>

        }
    }

    public static class WordFrequenceInDocReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable wordSum = new IntWritable();

        public WordFrequenceInDocReducer() {
        }

        /**
         * @param key is the key of the mapper
         * @param values are all the values aggregated during the mapping phase
         * @param context contains the context of the job run
         *
         *      PRE-CONDITION: receive a list of <"text",[1, 1, 1, ...]> pairs
         *        <"text", [1, 1]>
         *
         *      POST-CONDITION: emit the output a single key-value where the sum of the occurrences.
         *        <"text", 2>
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //write the key and the adjusted value (removing the last comma)
            this.wordSum.set(sum);
            context.write(key, this.wordSum);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        Job job = new Job(conf, "Word Frequence In Document");

        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordFrequenceInDocMapper.class);
        job.setReducerClass(WordFrequenceInDocReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequenceInDocument(), args);
        System.exit(res);
    }
}
