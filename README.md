# ChangeStream
Package to migrate changes from one environment to the other. Uses and intermediate database to store the changes. There is a reader and a multithreaded writer.
Reader reads from source and pushes the changes to intermediate. Writer reads from intermediate and writes to target.

1. You need to run one instance  for reading and other for writing. You can see the options that you can pass to the jar using
```  
 java -jar ChangeStream.jar  -h
```
2. You have to pass the following options for the runner:

```
java -jar ChangeStream.jar  -type reader -srcDB <Name of Source DB> -srcColl <Name of Source Collection> -srcConn <Source MOngoDB Connection String> -streamDB <Name of the intermediate db to store changes> -streanColl <Name of collection for storing changes> -streamConn <Connection string for intermediate MongoDB instance>
 -nThreads <Number of threads default 16> -bsize <batch size for each write default is 2500>
```
3. You have to pass the following options for the writer :
```
java -jar ChangeStream.jar  -type writer -targetDB <Name of target DB> -targetColl <Name of target Collection> -targetConn <Target MOngoDB Connection String> -streamDB <Name of the intermediate db to store changes> -streanColl <Name of collection for storing changes> -streamConn <Connection string for intermediate MongoDB instance> -nThreads <Number of threads default 16>
```

4. When you want to Stop streaming, you may have one last batch that is not applied yet. Stop the previous reader job and then reduce the bsize to 1 and pass the resumeToken.

So here is how you get the latest resume token from the intermediate collection:
 ```
 db.collection.find().sort({$natural:-1}).limit(1)
```
The output in mongo shell will look like this:


```
{ "_id" : ObjectId("5a5ffd959cc545041fb1655c"), "resumeToken" : { "_data" : BinData(0,"glpf/ZQAAAAURmRfaWQAZFpf/ZRZQf7HjFuQcgBaEATVeznc1WVNqrG9KqNab5iABA==") }, "thread" : 14, "payload" : { "_id" : ObjectId("5a5ffd945941fec78c5b9072"), "a" : 19, "b" : ISODate("2018-01-18T01:51:16.737Z"), "c" : BinData(4,"0011") }, "applied" : "Y" }
```


Copy the value of resumeToken and pass that to the java program like this:


```
java -jar ChangeStream.jar  -type reader -srcDB <Name of Source DB> -srcColl <Name of Source Collection> -srcConn <Source MOngoDB Connection String> -streamDB <Name of the intermediate db to store changes> -streanColl <Name of collection for storing changes> -streamConn <Connection string for intermediate MongoDB instance>
 -nThreads <Number of threads default 16> -bsize 1 -token "{ \"_data\" : BinData(0,\"glpf/ZQAAAAURmRfaWQAZFpf/ZRZQf7HjFuQcgBaEATVeznc1WVNqrG9KqNab5iABA==\") }"
```




5. I am assuming that there are no hard data deletes happening in the system. If there are then we will need to account for that in the code.
