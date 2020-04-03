package com.wenbin.fileinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Wenbin.Yang
 * @create 2020-04-02 21:17
 */
public class FileInput {
    public static class FileMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

            // 获取数据
            String line = value.toString();

            k.set(line);
            context.write(k, null);
        }
    }


    /**
     * The type Int sum reduce.
     */
    public static class FileReducer extends Reducer<Text, IntWritable, Text, IntWritable>{


        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

        }
    }


    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException            the io exception
     * @throws ClassNotFoundException the class not found exception
     * @throws InterruptedException   the interrupted exception
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setNumReduceTasks(0);
        // 2 设置jar加载路径
        job.setJarByClass(FileInput.class);

        // 3 设置map和reduce类
        job.setMapperClass(FileMapper.class);
        job.setReducerClass(FileReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }

}
