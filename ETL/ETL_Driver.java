package ETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ETL_Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"", ""};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //驱动器
        job.setJarByClass(ETL_Driver.class);
        job.setMapperClass(ETL_Mapper.class);

        //最终输出结果：直接跳过Reducer阶段
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //setInputPaths:可变参数
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);

    }

}
