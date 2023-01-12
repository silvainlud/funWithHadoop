```bash
hdfs dfs -rm -r /result.csv
hadoop jar FunWithMapReduce-1.0-SNAPSHOT.jar edu.reduce.map.fun.App hdfs://localhost:9000/games.csv hdfs://localhost:9000/result.csv
```