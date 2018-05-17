# 基于MapReduce实现物品协同过滤算法（ItemCF）

## 具体参考[博客](https://blog.csdn.net/u011254180/article/details/80353543)


#### 补充：hadoop三种执行方式

MR执行环境有两种：本地测试环境，服务器环境
	
本地测试环境(windows)：
	在windows的hadoop目录bin目录有一个winutils.exe
  
	1、在windows下配置hadoop的环境变量
	2、拷贝debug工具(winutils.ext)到HADOOP_HOME/bin
	3、修改hadoop的源码 ，注意：确保项目的lib需要真实安装的jdk的lib
	4、MR调用的代码需要改变：
		a、src不能有服务器的hadoop配置文件
		b、在调用是使用：
		  Configuration config = new  Configuration();
		  config.set("fs.defaultFS", "hdfs://node7:8020");
		  config.set("yarn.resourcemanager.hostname", "node7");

服务器环境：
首先需要在src下放置服务器上的hadoop配置文件

1、在本地直接调用，执行过程在服务器上（真正企业运行环境）

	a、把MR程序打包（jar），直接放到本地
	b、修改hadoop的源码 ，注意：确保项目的lib需要真实安装的jdk的lib
	c、增加一个属性：config.set("mapred.jar", "C:\\Users\\Administrator\\Desktop\\wc.jar");
	d、本地执行main方法，servlet调用MR。
	
	
2、直接在服务器上，使用命令的方式调用，执行过程也在服务器上

	a、把MR程序打包（jar），传送到服务器上
	b、通过： hadoop jar jar路径  类的全限定名
	       如：hadoop jar wc.jar com.zxl.mr.wc.RunJob
注：一些jar包可以通过 java jar xxx.jar 运行,使用hadoop则默认会把hadoop所依赖包添加
	
