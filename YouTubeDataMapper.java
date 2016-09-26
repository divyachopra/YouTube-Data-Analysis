import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class YouTubeDataMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, FloatWritable> {

	Text name = new Text();
	@Override
	public void map(LongWritable key, Text value,OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
		String strValue = value.toString();
		float val =0;
		if(strValue.length() >0){
		String line[]= strValue.toString().split("\t");
		if (line.length>=7)
		{
		name.set(line[0]);
		if(line[6] .matches("\\d+.+"))
		{
			 val = Float.parseFloat(line[6]);
			
		}
		
		}
		}
		
		output.collect(name, new FloatWritable(val));
	}

}
