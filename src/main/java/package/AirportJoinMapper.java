import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportJoinMapper extends Mapper<LongWritable, Text, KeyValueWritableComparable , Text> {
    private static final int AIRACE_ID_NUMBER = 0;
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] airraces = line.split(",");
        if (key.get() > 0) {
            String airportName = "";
            for (int i = 1 ; i < airraces.length; i++)
                airportName += airraces[i];
            int airaceID = Integer.parseInt(airraces[AIRACE_ID_NUMBER].replace("\"" , ""));
            context.write(new KeyValueWritableComparable(airaceID, 0),
                    new Text(airportName.replace("\"" , "")));
        }
    }
}
