package com.youku.data;

/**
 * Created by asha on 16-4-19.
 */
import org.apache.hadoop.hdfs.server.namenode.Content;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class GetMidDataReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException {
        int count =0;
        for (IntWritable myvalue : value){
            count += myvalue.get();
        }
        context.write(key,new IntWritable(count));
    }
}
