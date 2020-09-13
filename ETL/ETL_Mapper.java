package ETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETL_Mapper extends Mapper<LongWritable , Text , Text, NullWritable> {
    Text k =new Text();
    @Override
    protected void map(LongWritable key,
                       Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineLog = line.split(" ");
        boolean b = parseLogs(lineLog,context);
        if (!b){
            return;
        }
        k.set(line);
        context.write(k,NullWritable.get());
        //super.map(key, value, context);
    }
    public boolean parseLogs(String[] lineLog, Context context){
        if (lineLog.length>11){
            context.getCounter("map","true").increment(1);
            return true;
        }
        else {
            context.getCounter("map","false").increment(1);
            return false;
        }
    }
}