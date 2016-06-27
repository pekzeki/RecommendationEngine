package tr.edu.sabanci.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by hduser on 8/23/15.
 */
public class ColdStartReducer extends
        Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text text, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double rating = 0;
        int counter = 0;
        for (DoubleWritable value : values) {
            rating += value.get();
            counter++;
        }
        context.write(text, new DoubleWritable(rating/counter));
    }

}