import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Ngram {

	static String stringJoin(Collection<?> s, String delimiter) {
	     StringBuilder builder = new StringBuilder();
	     Iterator<?> iter = s.iterator();
	     while (iter.hasNext()) {
	         builder.append(iter.next());
	         if (!iter.hasNext()) {
	           break;                  
	         }
	         builder.append(delimiter);
	     }
	     return builder.toString();
	 }
	
	public static class NgramMapper extends
			Mapper<LongWritable, Text, TextIntPair, TextIntPair> {
		
		private HashSet<String> query; 
		private int n;
		private TextIntPair current = new TextIntPair();

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			query = new HashSet<String>(Arrays.asList(conf.get("query").split(",")));
			n = conf.getInt("n", 0);
		}
	
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String page = value.toString();
			String title = null;
			Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
			Matcher titleMatcher = titlePattern.matcher(page);
			if (titleMatcher.find() && (titleMatcher.groupCount() > 0)) {
				 title = titleMatcher.group(1);
				 page = page.substring(titleMatcher.end());
			}
			if (title == null)
				return;
			
			Tokenizer tokenizer = new Tokenizer(page);
			LinkedList<String> ngram = new LinkedList<String>();
			int cnt = 0;
			while (tokenizer.hasNext()) {
				String token = tokenizer.next();
				ngram.addLast(token);
				while (ngram.size() > n) {
					ngram.removeFirst(); 
				}
				if (ngram.size() == n) {
					if (query.contains(stringJoin(ngram, " "))) {
						cnt++;
					}
				}
			}
			if (cnt > 0) {
				current.getFirst().set(title);
				current.getSecond().set(cnt);
				context.write(current, current);
			}
		}
	}
	public static class NgramPartitioner extends Partitioner<TextIntPair, TextIntPair> {
		@Override
		public int getPartition(TextIntPair key, TextIntPair value, int numPartitions) {
			return 0;
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(TextIntPair.class, true);
		}
	}
	
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(TextIntPair.class, true);
		}
		
		@Override
		@SuppressWarnings("unchecked")
		public int compare(WritableComparable w1, WritableComparable w2) {
			return 0;
		}
	}
	
	public static class NgramReducer extends
			Reducer<TextIntPair, TextIntPair, Text, IntWritable> {

		private int k;
		@Override
		public void setup(Context context) {
			k = context.getConfiguration().getInt("topk", 0);
		}
		@Override
		public void reduce(TextIntPair key, Iterable<TextIntPair> values,
				Context context) throws IOException, InterruptedException {
			int cnt = 0;
			for (TextIntPair w : values) {
				if (cnt >= k)
					break;
				context.write(w.getFirst(), w.getSecond());
				cnt++;
			}
		}
	}
	
	public static HashSet<String> readQuery(int n, Path file, Configuration conf) throws IOException {
		FileSystem fs = file.getFileSystem(conf);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file), "UTF-8"));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = reader.readLine()) != null) {
			sb.append(line + "\n");
		}
		reader.close();

		Tokenizer tokenizer = new Tokenizer(sb.toString());
		LinkedList<String> ngram = new LinkedList<String>();
		HashSet<String> query = new HashSet<String>();
		while (tokenizer.hasNext()) {
			String token = tokenizer.next();
			ngram.addLast(token);
			while (ngram.size() > n) {
				ngram.removeFirst(); 
			}
			if (ngram.size() == n) {
				query.add(stringJoin(ngram, " "));
			}	
		}
		return query;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = new Path(args[3]).getFileSystem(conf);
		fs.delete(new Path(args[3]), true);
		
		
		int n = Integer.parseInt(args[0]);
		conf.setInt("n", n);
		conf.setInt("topk", 20);
		HashSet<String> query = readQuery(n, new Path(args[1]), conf);
		conf.set("query", stringJoin(query, ","));
		conf.set("xmlStart", "<page>");
		conf.set("xmlEnd", "</page>");
		
		Job job = new Job(conf, "Ngram");
		job.setJarByClass(Ngram.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapOutputKeyClass(TextIntPair.class);
		job.setMapOutputValueClass(TextIntPair.class);

		job.setMapperClass(NgramMapper.class);
		job.setPartitionerClass(NgramPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setReducerClass(NgramReducer.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
	}

}
