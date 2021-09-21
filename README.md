# PageRank

For Hadoop Page Rank running:

1) open the VM with the IP = 172.16.3.166
2) go on hadoop with ssh hadoop@172.16.3.166
3) start-dfs.sh
4) start-yarn.sh
5) go into the pagerank folder -> cd pagerank
6)compile the code with -> mvn clean package
7)run of the application passing parameters <input name>, <output name>, <alpha>, <iterations> -> hadoop jar target/pagerank-1.0-SNAPSHOT.jar it.unipi.hadoop.PageRank synthetic.txt output 0.15 3
