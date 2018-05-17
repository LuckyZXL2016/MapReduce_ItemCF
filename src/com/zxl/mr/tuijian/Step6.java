package com.zxl.mr.tuijian;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 按照推荐得分降序排序，每个用户列出10个推荐物品
 * @author ZXL
 *
 */
public class Step6 {
	private final static Text K = new Text();
	private final static Text V = new Text();
	
	public static boolean run(Configuration config, Map<String, String> paths) {
		try {
			FileSystem fs = FileSystem.get(config);
			Job job = Job.getInstance(config);
			job.setJobName("step6");
			job.setJarByClass(StartRun.class);
			job.setMapperClass(Step6_Mapper.class);
			job.setReducerClass(Step6_Reducer.class);
			job.setSortComparatorClass(NumSort.class);
			job.setGroupingComparatorClass(UserGroup.class);
			job.setMapOutputKeyClass(PairWritable.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat
					.addInputPath(job, new Path(paths.get("Step6Input")));
			Path outpath = new Path(paths.get("Step6Output"));
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	static class Step6_Mapper extends Mapper<LongWritable, Text, PairWritable, Text>{
		protected void map(LongWritable key, Text value, 
				Context context)
				throws IOException, InterruptedException {
			String[] tokens = Pattern.compile("[\t,]").split(value.toString());
			String u = tokens[0];
			String item = tokens[1];
			String num = tokens[2];
			PairWritable k =new PairWritable();
			k.setUid(u);
			k.setNum(Double.parseDouble(num));
			V.set(item+":"+num);
			context.write(k, V);
		}
	}
	
	//注意，排序是按照用户id和num排序，分组是按照用户id排序
	static class Step6_Reducer extends Reducer<PairWritable, Text, Text, Text> {
		protected void reduce(PairWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int i=0;
			StringBuffer sb =new StringBuffer();
			for(Text v :values){
				if(i==10)
					break;
				sb.append(v.toString()+",");
				i++;
			}
			K.set(key.getUid());
			V.set(sb.toString());
			context.write(K, V);
		}

	}
	
	static class PairWritable implements WritableComparable<PairWritable>{
		
//		private String itemId;
		private String uid;
		private double num;
		
		public String getUid() {
			return uid;
		}
		public void setUid(String uid) {
			this.uid = uid;
		}
		public double getNum() {
			return num;
		}
		public void setNum(double num) {
			this.num = num;
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(uid);
			out.writeDouble(num);
		}

		public void readFields(DataInput in) throws IOException {
			this.uid = in.readUTF();
			this.num = in.readDouble();
		}

		public int compareTo(PairWritable o) {
			int r = this.uid.compareTo(o.getUid());
			if(r == 0){
				return Double.compare(this.num, o.getNum());
			}
			return r;
		}
		
	}
	
	static class NumSort extends WritableComparator{
		public NumSort(){
			super(PairWritable.class, true);
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 =(PairWritable) a;
			PairWritable o2 =(PairWritable) b;
			
			int r =o1.getUid().compareTo(o2.getUid());
			if(r==0){
				return -Double.compare(o1.getNum(), o2.getNum());
			}
			return r;
		}
	}
	
	static class UserGroup extends WritableComparator{
		public UserGroup(){
			super(PairWritable.class,true);
		}
		
		public int compare(WritableComparable a, WritableComparable b) {
			PairWritable o1 =(PairWritable) a;
			PairWritable o2 =(PairWritable) b;
			//根据用户id分组
			return o1.getUid().compareTo(o2.getUid());
		}
	}
}
