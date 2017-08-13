# PageRank

The goal of this programming assignment is to compute the PageRanks of an input set of hy-
perlinked Wikipedia documents using Hadoop MapReduce. The PageRank score of a web page
serves as an indicator of the importance of the page. Many web search engines (e.g., Google) use
PageRank scores in some form to rank user-submitted queries. The goals of this assignment are to:
1. Understand the PageRank algorithm and how it works in MapReduce.
2. Implement PageRank and execute it on a large corpus of data.
3. Examine the output from running PageRank on Simple English Wikipedia to measure the
relative importance of pages in the corpus.


** How to run the program *

Cluster:

Input Directory: /user/akadam3/PageInput
Output Directory: /user/akadam3/PageOutput

1. Compile a file

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/PageRank.java -d build -Xlint

2. Create Jar file

jar -cvf pagerank.jar -C build/ .

3. Run a file

hadoop jar pagerank.jar org.myorg.pagerank /user/akadam3/pageInput/simple-wiki /user/akadam3/PageOutput

4. Copy output to a file

hadoop fs -cat /user/akadam3/PageOutput/* >simpleWikiOutput.txt


Local:

Input Directory: /user/cloudera/PageInput

Output Directory: /user/cloudera/PageOutput

1. Compile a file

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/PageRank.java -d build -Xlint

2. Create Jar file

jar -cvf pagerank.jar -C build/ .

3. Run a file

hadoop jar pagerank.jar org.myorg.pagerank /user/cloudera/pageInput/wiki-micro.txt /user/akadam3/PageOutput

4. Copy output to a file

hadoop fs -cat /user/cloudera/PageOutput/* >wikiMicroOutput.txt


***** Delete all intermediate and final output files before running any program again, if there are any.
hadoop fs -rm -r /user/cloudera/pageOutput*
 
