package com.zxl.mr.tuijian;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class StartRun {
	
	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://node5:8020");
		config.set("yarn.resourcemanager.hostname", "node5");
		//所有mr的输入和输出目录定义在map集合中
		Map<String, String> paths = new HashMap<String, String>();
		paths.put("Step1Input", "/usr/input/(sample)sam_tianchi_2014002_rec_tmall_log.csv");
		paths.put("Step1Output", "/usr/output/step1");
		paths.put("Step2Input", paths.get("Step1Output"));
		paths.put("Step2Output", "/usr/output/step2");
		paths.put("Step3Input", paths.get("Step2Output"));
		paths.put("Step3Output", "/usr/output/step3");
		paths.put("Step4Input1", paths.get("Step2Output"));
		paths.put("Step4Input2", paths.get("Step3Output"));
		paths.put("Step4Output", "/usr/output/step4");
		paths.put("Step5Input", paths.get("Step4Output"));
		paths.put("Step5Output", "/usr/output/step5");
		paths.put("Step6Input", paths.get("Step5Output"));
		paths.put("Step6Output", "/usr/output/step6");

//		Step1.run(config, paths);
//		Step2.run(config, paths);
//		Step3.run(config, paths);
//		Step4.run(config, paths);
//		Step5.run(config, paths);
		Step6.run(config, paths);
	}
	
	public static Map<String, Integer> R = new HashMap<String, Integer>();
	static {
		R.put("click", 1);
		R.put("collect", 2);
		R.put("cart", 3);
		R.put("alipay", 4);
	}
}
