package tr.edu.sabanci.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by hduser on 8/23/15.
 */
public class UserCategoryMatrixReducer extends
        Reducer<Text, Text, Text, Text> {

    public void reduce(Text text, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuffer stringBuffer = new StringBuffer("");
        for (Text value : values) {
            stringBuffer.append(value);
            stringBuffer.append(",");
        }
        context.write(text, new Text(stringBuffer.toString()));
    }

}
