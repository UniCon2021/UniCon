# UniCon

- UniCon is a tool for finding all connected components in a tremendous graph.
- Given an undirected graph, UniCon maps each node to the most preceding node in the same connected components.
- It runs in distributed system, Hadoop. 

## Build

UniCon uses maven to manage dependencies and build the whole project. To build the project, type the following command in terminal:

```bash
mvn install -Dmaven.test.skip=true
```

## Run UniCon
- Hadoop should be installed in your system.
- Prepare an edge list file of a graph in a HDFS directory. ex) multiple.tsv
- example (multiple.tsv)
```
0   1
2   3
3   4
4   5
5   6
7   8
8   9
```

- execute UniCon on Hadoop 
```
hadoop jar target/UniCon-1.0-SNAPSHOT-jar-with-dependencies.jar [CLASS NAME] [OPTIONS] [INPUT] [OUTPUT] 

Class names:
[# nodes < 2,147,483,647]
    UniCon-base: unicon.intRange.intRange.base.UniConBase
    UniCon-opt: unicon.intRange.intRange.opt.UniConOpt
[# nodes >= 2,147,483,647]
    UniCon-base: unicon.longRange.intRange.base.UniConBase
    UniCon-opt: unicon.longRange.intRange.opt.UniConOpt
  
Options:
[base version]
    -DnumPartitions        the number of partitions
    -DnumNodes             the number of nodes
[opt version]
    -DnumPartitions        the number of partitions
    -DnumNodes             the number of nodes
    -Dthreshold            the threshold for LocalCC optimization (default: 0)
    
Example:
hadoop jar target/UniCon-1.0-SNAPSHOT-jar-with-dependencies.jar unicon.intRange.opt.UniConOpt -Dmapred.reduce.tasks=80 -DnumPartitions=80 -DnumNodes=4847571 -Dthreshold=200000000 path/to/input/file path/to/output/file
```

## Datasets
### Real world graphs
| Name        | #Nodes      | #Edges        | Description                                                 | Source                           |
|-------------|-------------|---------------|-------------------------------------------------------------|----------------------------------|
| LiveJournal     | 4,847,571  | 68,993,773 | LiveJournal online social network                           | [SNAP](http://snap.stanford.edu/data/soc-LiveJournal1.html) |
| Twitter     | 41,652,230  | 1,468,365,182 | Twitter follower-followee network                           | [Advanced Networking Lab at KAIST](http://an.kaist.ac.kr/traces/WWW2010.html) |
| Friendster  | 65,608,366  | 1,806,067,135 | Friendster online social network                            | [SNAP](http://snap.stanford.edu/data/com-Friendster.html)                             |
| SubDomain   | 89,247,739  | 2,043,203,933 | Domain level hyperlink network on the Web                   | [Web Data Commons](http://webdatacommons.org/hyperlinkgraph/index.html)                   |
| gsh-2015    | 988,490,691 | 33,877,399,152 | Domain level hyperlink network on the Web                     | [WebGraph](http://law.di.unimi.it/webdata/gsh-2015/)                   |
| ClueWeb    | 6,257,706,595 | 71,746,553,402 | Page level hyperlink network on the Web                     | [Lemur Project](http://www.lemurproject.org/clueweb12/webgraph.php/)                   |
| Hyperlink14   | 3,443,082,324 | 128,736,914,167 | Page level hyperlink network on the Web                     | [Web Data Commons](http://webdatacommons.org/hyperlinkgraph/)                   |

### Synthetic graphs
RMAT-k for k ∈ {21, 23, 25, 27, 29, 31, 33} is a synthetic graph following RMAT model.
We set RMAT parameters (a, b, c, d) to (0.57, 0.19, 0.19, 0.05).
| Name      | #Nodes      | #Edges        |
|-----------|-------------|---------------|
| RMAT-21 | 1,114,816 | 31,457,280 |
| RMAT-23 | 4,120,785 | 125,829,120 |
| RMAT-25 | 15,212,447 | 503,316,480 |
| RMAT-27 | 56,102,002 | 2,013,265,920 |
| RMAT-29 | 207,010,037| 8,053,063,680 |
| RMAT-31 | 762,829,446 | 32,212,254,720 |
| RMAT-33 | 1,090,562,291 | 128,849,018,880 |

## Experiments
### Figure 6: The running time of UniCon-opt on various τ. 
![threshold-1](https://user-images.githubusercontent.com/69777365/200504614-4cb725a3-78ab-405e-8a9a-47f16f02aa4d.png)

### Figure 7: The running time of UniCon-opt and UniCon-base increases marginally as ρ increases.
![partition_time-1](https://user-images.githubusercontent.com/69777365/200504680-014131e8-c19d-4cdc-b709-ad68c11ec6e5.png)

### Figure 8: UniStar reduces both the input and intermediate data sizes by up to 4x and 8x, respectively, compared to UniStar-naïve. o.o.m.: out-of-memory.
![intermediate_edges-1](https://user-images.githubusercontent.com/69777365/200504747-4843fff0-27dd-41fd-a4a8-defb193aada3.png)

### Figure 9: The running time of UniStar and UniStar-naïve, and its' cumulative sums, respectively. UniStar outperforms UniStar-naïve, which fails on CW.
![intermediate_speed-1](https://user-images.githubusercontent.com/69777365/200504805-b8ce5afb-edcf-4e45-96fd-e9a5610e954d.png)

### Figure 10: Filtering dispensable edges (denoted by bars), UniCon-opt shrinks the input size (denoted by lines) by 80.4% on average in each distributed operation.
![edgefiltering-1](https://user-images.githubusercontent.com/69777365/200504874-88fa4d98-af39-4a57-9e90-06fd383a24c5.png)

### Figure 11: By the edge filtering, the running time of UniStar-opt drops quickly showing the best performance.
![edge_filtering_time-1](https://user-images.githubusercontent.com/69777365/200504927-6c9e24e9-00bd-4683-8e1b-b1cdbf19e8ca.png)

### Figure 12: The size of data that UniCon-opt stores in memory with HybridMap, a hash table, and an array, respectively. HybridMap stores as little data as a hash table.
![hybrid_memory-1](https://user-images.githubusercontent.com/69777365/200504977-d2d6f523-fea0-4bbf-a771-65d78ce0765d.png)

### Figure 13: The running time of UniStar-opt with HybridMap, a hash table, and an array, respectively, in each round. UniCon-opt with HybridMap outperforms Unicon-opt with hash tables when the graph is large enough. UniCon-opt with arrays occurs an out-of-memory error on CW.
![hybrid_speed-1](https://user-images.githubusercontent.com/69777365/200505062-f0ab61a8-3280-44b9-a829-1285eba19ba4.png)

### Figure 14: Data and machine scalability.(left) UniCon handles up to 4096x larger graphs than competitors. (right) UniCon-opt with optimal τ shows the best performance regardless of the number of machines.
![data_scale_all](https://user-images.githubusercontent.com/69777365/129442519-73ad61b2-84aa-4b72-a554-c121217361f0.png)

### Figure 15: The relative running time, compared to UniCon-opt with optimal τ, of competitors on real-world graphs. o.o.m.: out-of-memory error.
![total_running_time](https://user-images.githubusercontent.com/69777365/129442524-2488cf03-3cac-4d07-bfef-a267c773703a.png)

### Figure 16: The numbers of distributed operations required by all algorithm except Rem on real-world graphs. UniCon-opt requires up to 11 fewer rounds than competitors.
![distributed_operations](https://user-images.githubusercontent.com/69777365/129442529-6b34e4c6-b0d2-4eb5-b473-042c079f1f21.png)
