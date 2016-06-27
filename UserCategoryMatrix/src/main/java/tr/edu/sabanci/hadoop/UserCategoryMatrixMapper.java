package tr.edu.sabanci.hadoop;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by hduser on 8/23/15.
 */
public class UserCategoryMatrixMapper extends
        Mapper<Object, Text, Text, Text> {

    private Text userId = new Text();
    private Text category = new Text();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] csv = value.toString().split("\t");
        userId.set(csv[0]);
        if(!csv[9].equals("[]")) {
            category.set(csv[9].replace("\'", "").replace("\"","").replace("[", "").replace("]", ""));
            context.write(userId, category);
        }

    }
}

