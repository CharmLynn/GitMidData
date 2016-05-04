package com.youku.data;

/**
 * Created by asha on 16-4-19.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
public class GetMidDataMapper extends Mapper<Object,Text,Text,IntWritable>{
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
        String pvcount = "";
        String urlcount = "";
        String[] midvalue = value.toString().trim().split("\t");
        if(midvalue.length==23) {
            pvcount = midvalue[3];
            urlcount = midvalue[4];
//            context.write(new Text(pvcount),new IntWritable(1));
            context.write(new Text("pv"+"\t"+pvcount),new IntWritable(1));
            context.write(new Text("urlcount"+"\t"+urlcount),new IntWritable(1));
        }
    }
}
