

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class YouTubeDriver {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(YouTubeDriver.class);
		conf.setJobName("DataAnalysis");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(YouTubeMapper.class);
		conf.setReducerClass(YouTubeReducer.class);	
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		
	}

}
