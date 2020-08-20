# UniCon

- UniCon is a tool for finding all connected components in a web-scale graph.
- Given an undirected graph, UniCon maps each node to the most preceding node in the same connected components.
- It runs in distributed system, Hadoop. 

## Build

UniCon uses maven to manage dependencies and build the whole project. To build the project, type the following command in terminal:

```bash
maven install -Dmaven.test.skip=true
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
hadoop jar UniCon-1.0.jar unicon.intRange.opt.UniConOpt -DnumPartitions=80 -Dthreshold=10000 -DnumNodes=1696415 graphs/sk.tsv dongchime_result/sk.out


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
hadoop jar target/UniCon-1.0-SNAPSHOT-jar-with-dependencies.jar unicon.intRange.opt.UniConOpt -Dmapred.reduce.tasks=80 -DnumPartitions=80 -DnumNodes=720242173 -DlocalThreshold=20000000 path/to/input/file path/to/output/file
```

## Datasets
### Real world graphs
| Name        | #Nodes      | #Edges        | Description                                                 | Source                           |
|-------------|-------------|---------------|-------------------------------------------------------------|----------------------------------|
| Twitter     | 41,652,230  | 1,468,365,182 | Twitter follower-followee network                           | [Advanced Networking Lab at KAIST](http://an.kaist.ac.kr/traces/WWW2010.html) |
| Friendster  | 65,608,366  | 1,806,067,135 | Friendster online social network                            | [SNAP](http://snap.stanford.edu/data/com-Friendster.html)                             |
| SubDomain   | 89,247,739  | 2,043,203,933 | Domain level hyperlink network on the Web                   | [Yahoo Webscope](http://webscope.sandbox.yahoo.com/)                   |
| YahooWeb    | 720,242,173 | 6,636,600,779 | Page level hyperlink network on the Web                     | [Yahoo Webscope](http://webscope.sandbox.yahoo.com/)                   |
| ClueWeb    | 6,257,706,595 | 71,746,553,402 | Page level hyperlink network on the Web                     | [Lemur Project](http://www.lemurproject.org/clueweb12/webgraph.php/)                   |

### Synthetic graphs
CW/k for k ∈ {2, 4, 8, 16, 32, 64, 128} is a subgraph induced by |V |/k sampled nodes.
| Name      | #Nodes      | #Edges        |
|-----------|-------------|---------------|
| CW/128 | 48,888,332 | 3,829,757 |
| CW/64 | 97,776,665 | 16,916,884 |
| CW/32 | 195,553,331 | 68,557,884 |
| CW/16 | 391,106,662 | 278,037,241 |
| CW/8 | 782,213,324 | 1,132,445,224 |
| CW/4 | 1,564,426,648 | 4,515,676,428 |
| CW/2 | 3,128,853,297 | 17,928,970,934 |
