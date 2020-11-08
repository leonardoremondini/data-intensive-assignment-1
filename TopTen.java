package id2221.topten;

import java.util.Iterator;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//System.out.println(value.toString()+"\n");

		try{
			String line =  value.toString();
				if(line.matches(".*Id=\"-?\\d+\".*")){
					Map<String, String> map = transformXmlToMap(line);
					repToRecordMap.put(Integer.parseInt(map.get("Reputation")),new Text(map.get("Id")));
			}

	 } catch (RuntimeException e) {
		 e.printStackTrace();
	 }

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
				Iterator<Integer> iterator =  repToRecordMap.descendingKeySet().iterator();
				for(int i=0;i<10 && iterator.hasNext(); i++){
					 Integer rep = iterator.next();
					 context.write(NullWritable.get(), new Text(repToRecordMap.get(rep)+" "+rep));
			 	}
			}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			try{
								
				for (Text val : values) {
					//The first number is the id and the second one is the reputation, split the string on whitespace to get the values.
					String[] splited = val.toString().split("\\s+");
					repToRecordMap.put(Integer.parseInt(splited[1]),new Text(splited[0]));
	
				}


				Iterator<Integer> iterator =  repToRecordMap.descendingKeySet().iterator();
		
				for(int i=0; iterator.hasNext(); i++){
					Integer rep = iterator.next();
					String id = repToRecordMap.get(rep).toString();
                
					Put insHbase = new Put(Bytes.toBytes(i+1));
					insHbase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"),Bytes.toBytes(Integer.toString(rep)));
					insHbase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"),Bytes.toBytes(id));
					context.write(NullWritable.get(), insHbase);
				}


			} catch (Exception e) {
				e.printStackTrace();
			}		
			
		}
	}


	public static void main(String[] args) throws Exception {

			Configuration conf = HBaseConfiguration.create();
			Job job = Job.getInstance(conf, "Topten");
			job.setJarByClass(TopTen.class);

			job.setMapperClass(TopTenMapper.class);
			job.setNumReduceTasks(1);
			TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			//FileSystem.get(conf).delete(new Path(args[1]),true);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}