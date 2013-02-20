CS 149 Programming Assignment 3

Botao Hu (botaohu@stanford.edu)

[Preprocess]

To split the input into pages which are marked by <page> and </page>, we implement a class XmlInputFormat which extends TextInputFormat. XmlInputFormat helps to extract the content between <page> and </page> as a Text class and input it to the mapper. Our implementation can address the page which is splited due to the split of the chunk . 

Our implementation is a modified version of https://svn.apache.org/repos/asf/mahout/trunk/integration/src/main/java/org/apache/mahout/text/wikipedia/XmlInputFormat.java

[Job]
Before submitting the job, we read the query file and split it into a set of n-grams and stringify the set and store it into Configuration. 

[Mapper]
From Configuration, we recover the set of n-grams of the query as a HashSet. For every page, we look up every n-grams of that page in the HashSet of n-grams of the query. If it exists, we count one into the variable Count.

For every page, we push <Title, Count> into a TreeSet, which is ordered set by the decreasing order of the Count. We maintain the size of the TreeSet less than k items, i.e., once the size of the TreeSet greater than k, we remove the smallest one. 

During cleanup() of the mapper, we  finally output top-k items in the TreeSet. by emitting (Key: Null, Value: <Title, Count>). 

[Reducer]
Since the keys are all Null, all data will input into one reducer. This reducer will do the same job of the mapper, i.e., maintaining a TreeSet to remain the top-k items, and outputs top-k items in cleanup() of the reducer.

[Scalability]
The inputs are splited into pages and thus could handled by multiple mappers simultaneously. The complexity of each mapper is n log(k) / P = O(n / P). 
Thus, the running time will be bounded by O(q + n / P + log P). 

[Running & Results] 
Run this command to submit the job on EC2.
qsub pa3.pbs
The result outputs to output1/ for query1 and output2/ for query2.
The log is recorded in pa3.pbs.e2356. 
From the log, we can see that the actual running time is about 8 mins (20:36:15 to 20:44:14 for query1, 20:44:19 to 20:52:20 for query2. 


