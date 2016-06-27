package tr.edu.sabanci.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hduser on 8/23/15.
 */
public class ColdStartMapper extends
        Mapper<Object, Text, Text, DoubleWritable> {

    private Text businessId = new Text();
    private DoubleWritable star = new DoubleWritable();

    public List<String> getHubs(String city) {

        InputStream in = getClass().getResourceAsStream("/" + city+".csv");
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String> hubList = new ArrayList<String>();

        try {
            int counter = 0;
            String strLine;
            while((strLine = reader.readLine())!= null && counter < 20) {
                hubList.add(strLine.split("\t")[1]);
                counter++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return hubList;

    }

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        String category = conf.get("Category");
        String city = conf.get("City");
        List<String> hubList = getHubs(city);

        String[] csv = value.toString().split("\t");
        if (csv.length != 6) {
            System.out.println(csv.length);
            System.out.println(csv[0]);
            System.out.println(csv[1]);
            System.out.println(csv[2]);
            System.out.println(csv[3]);
            System.out.println(csv[4]);
            System.out.println(csv[5]);
            System.exit(0);
        }

        if (hubList.contains(csv[0])
                && csv[3].contains(category)
                && csv[4].equals(city)) {
            businessId.set(csv[2]);
            star.set(Double.parseDouble(csv[5]));
            context.write(businessId, star);
        }

    }

}

