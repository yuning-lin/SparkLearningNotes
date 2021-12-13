## 簡介
當資料集小可以用 Pandas DataFrame 處理即可  
但資料集大（如億級以上的資料）時可用 PySpark DataFrame 分散式處理  
PySpark 為一 Python API，利用 Apache Spark 運行 Python 程序以達到分散式平行運算的效果  

## PySpark 架構
* Apache Spark 是 master-slave 架構
* master 被稱為 Driver、slaves 被稱為 Workers
* 當運行 Spark 應用程序的時候，Driver 會創建一個應用程序的入口點
* 所有操作都會在工作節點上執行
* 資源的分派則有 Cluster Manager 做管理

    ```mermaid
    graph LR;
        Driver --> ClusterManager;
        ClusterManager-->WorkerNode1;
        ClusterManager-->WorkerNode2;
    ```

#### Spark 支援的 Cluster Manager
* Standalone：Spark 裡的簡單集群管理器，可輕鬆建立集群
* Apache Mesos：Mesos 為可以運行 Hadoop MapReduce 和 PySpark 應用程序的集群管理器
* Hadoop YARN：是 Hadoop 2 中的資源管理器，為最常被使用的集群管理器
* Kubernetes：為一用在自動部署、scaling、management of containerized applications 的開源系統
注：local 並不是集群管理器，但當用 local 做為 master() 的時候是為了在自己的電腦運行 Spark

## PySpark 模塊與套包
* pyspark.RDD
* pyspark.sql(DataFrame & SQL)
* pyspark.streaming
* pyspark.ml, pyspark.mllib
* GraphFrames
* pyspark.resource(PySpark 3.0)
* [third-party libraries](https://spark-packages.org/)

## Windows 上安裝 PySpark
需安裝
* [Python 3.6+](https://www.python.org/downloads/windows/)
* [Java 8+](https://www.oracle.com/java/technologies/downloads/#java8) 
  * 開啟 CMD 設置 JAVA_HOME 及 PATH 變數
  ```
  setx -m JAVA_HOME "C:\Program Files\Java\jdk1.8.0_311"
  setx -m PATH "%PATH%;%JAVA_HOME%\bin";
  ```
  * 確認設置 JAVA_HOME 及 PATH 變數是否有設置成功
  ```
  echo %JAVA_HOME% # 有顯示剛設置路徑即為成功
  echo %PATH% # 有顯示於 path 中即為成功
  java -version # 顯示 JAVA 版本
  ```
* [Apache Spark](https://spark.apache.org/downloads.html) 
  * 創建 `c:\apps` 路徑
  * spark-3.0.3-bin-hadoop2.7.tar 用 `7zip` 解壓縮至 `c:\apps` 並設置環境變數
  ```
  setx -m SPARK_HOME "C:\apps\spark-3.0.3-bin-hadoop2.7"
  setx -m HADOOP_HOME "C:\apps\spark-3.0.3-bin-hadoop2.7"
  setx -m PATH "%PATH%;%SPARK_HOME%\bin";
  ```
  * 確認設置有設置成功也可以從：開始＞系統內容＞進階＞環境變數＞系統變數（S），看變數是否有出現剛才的設置
* [下載和 Hadoop 相應的 winutils 版本](https://github.com/steveloughran/winutils) 並複製 `winutils.exe` 至 `C:\apps\spark-3.0.3-bin-hadoop2.7\bin`
* 開啟全新的 CMD＞將路徑移至 SPARK_HOME\bin＞鍵入 pyspark 以開啟 PySpark shell＞看到 Spark 字樣即為成功＞鍵入 exit() 以結束
* http://localhost:4040/jobs/ 可以見 Spark Web UI 可以用來監控應用程序

## 參考資源
* [PySpark Tutorial](https://sparkbyexamples.com/pyspark-tutorial/)
* [PySpark Cheat Sheet from DataCamp](https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf)
* [Installing Apache PySpark on Windows 10](https://towardsdatascience.com/installing-apache-pyspark-on-windows-10-f5f0c506bea1)
