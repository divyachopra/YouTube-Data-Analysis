

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class YouTubeDataDriverTwo {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(YouTubeDataDriverTwo.class);
		conf.setMapperClass(YouTubeDataMapper.class);
		conf.setReducerClass(YouTubeDataReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}

}
