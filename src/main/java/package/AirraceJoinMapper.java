import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;
import java.io.IOException;

public class AirraceJoinMapper extends Mapper<LongWritable, Text, KeyValueWritableComparable , Text> {
    static final int AIROPORT_ID_NUMBER = 14;
    static final int DELAY_TIME_NUMBER = 17;
    public static final String DELIMETER = ",";

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] oneRaceInfo = line.split(DELIMETER);
//        String isDelay = oneRaceInfo[17];
        if (key.get() > 0) {
//            int isCanceld = Integer.parseInt(oneRaceInfo[19]);

            if (!oneRaceInfo[AIROPORT_ID_NUMBER].equals("")
                    && !oneRaceInfo[DELAY_TIME_NUMBER].equals("")) {
                int airoportID = Integer.parseInt(oneRaceInfo[AIROPORT_ID_NUMBER]);
                double delayTime = Double.parseDouble(oneRaceInfo[DELAY_TIME_NUMBER]);
                if (delayTime > 0)
                    context.write(new KeyValueWritableComparable(airoportID , 1), new Text(String.valueOf(delayTime)));
            }
        }
    }
}
