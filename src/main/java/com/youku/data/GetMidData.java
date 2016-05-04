package com.youku.data;

/**
 * Created by asha on 16-4-19.
 */
import org.apache.avro.io.parsing.Symbol;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;


import java.io.IOException;


public class GetMidData {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length !=2) {
            System.err.println("Usage: GetDeatail data <inputfile path> <output filepath>");
            System.exit(-1);
        }
        //Init job
        Job job = Job.getInstance();
        job.setJarByClass(GetMidData.class);
        job.setJobName("Get Detaildata");
        //set input and output file or dir
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //set Mapper Class and Reducer Class
        job.setMapperClass(GetMidDataMapper.class);
        job.setReducerClass(GetMidDataReducer.class);
        //set Mapoutkey class and map out value class
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        System.out.println(job.waitForCompletion(true)?0:1);
    }
}
