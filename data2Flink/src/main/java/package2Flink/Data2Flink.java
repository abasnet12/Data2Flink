package package2Flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class Data2Flink {

	public static void main(String[] args) throws Exception 
	{
		final StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
		// checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
				
		// setting up the environment for global parameters
		env1.getConfig().setGlobalJobParameters(params);
		DataStream<String> data = env1.readTextFile(params.get("input"));
		
		
		DataStream<Tuple6<String,Double,Double, Double, Long, Long >> mapped = data.map(new Splitter());
		DataStream<Tuple6<String,Double,Double, Double, Long, Long >> filter = mapped.keyBy(0);
		data.print();
		env1.execute("Flight Data on FLink");

	}// main

	public static class Splitter implements MapFunction<String, Tuple6<String,Double,Double, Double, Long, Long >>
	{
		public Tuple6<String,Double,Double, Double, Long, Long > map (String value)
		{
			String[] words = value.split(",");
			return new Tuple6<String,Double,Double, Double, Long, Long >(words[1], Double.parseDouble(words[2]), Double.parseDouble(words[3]), Double.parseDouble(words[4]), Long.parseLong(words[5]),Long.parseLong(words[6]));
		}
		
	}// Splitter
	
	public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>
	{
		public Tuple2<String, Integer> map (String value)
		{
			return new Tuple2<String, Integer>(value,1);
		}
	}//
	
}// Data2Flink class
