import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class YouTubeDataReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	public void reduce(Text key, Iterator<FloatWritable> value,OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
		float sum = 0;
		while(value.hasNext())
		{
			sum +=value.next().get();
		}
		output.collect(key, new FloatWritable(sum));
	}

}
