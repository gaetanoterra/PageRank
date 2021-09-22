# PageRank

Instructions for the page rank algorithm applications running

# For Hadoop Page Rank running:

1) open the VM with the IP = 172.16.3.166
2) go on hadoop with ssh hadoop@172.16.3.166
3) start-dfs.sh
4) start-yarn.sh
5) go into the pagerank folder -> cd pagerank
6) compile the code with -> mvn clean package
7) run of the application passing parameters "input name", "output name", "alpha", "iterations" -> hadoop jar target/pagerank-1.0-SNAPSHOT.jar it.unipi.hadoop.PageRank synthetic.txt output 10
8) to read the output -> hadoop fs -cat output/sort/part* | head

# For Spark page rank running:

1) open the VM with the IP = 172.16.3.166
2) go on hadoop with ssh hadoop@172.16.3.166
3) start-dfs.sh
4) start-yarn.sh
5) go into the pagerank folder for spark application -> cd pagerank_spark
6) run of the application passing parameters "input name", "output name", "alpha", "iterations" -> spark-submit PageRankSpark.py synthetic.txt spark_output 0.15 10
7) to read the output -> hadoop fs -cat output_name/part* | head
