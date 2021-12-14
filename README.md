## SparkLearningNotes
## 名詞介紹
### RDD（Resilient Distributed Dataset）：
  * 是 Spark 的基本數據結構
  * 為可容錯、不可變的物件集合。不可變表示一但創建 RDD 就不可以改變
  * 很像 Python list 的物件集合，只是 RDD 可以分散在多個物理伺服器上的多個進程上計算；但 Python 僅能在單一進程處理
  * 當對 RDD 做轉換時 PySpark Default 會進行平行運算
  * 優勢：
    
    |名詞 |介紹|
    |-------------------------------------|------------------------------------------| 
    |運用內存處理（In-Memory Processing）|在轉換間可以快取內存重複使用前次計算，相較於 MapReduce 需要密集的 I/O 快|
    |不可變（Immutability）|當轉換 RDD 即是創造一個新的 RDD 並維護 RDD 的族譜|
    |可容錯（Fault Tolerance）|當任何 RDD 操作失敗，會自動從其他 partition 重載資料；當 PySpark 在集群上運行應用程序任務失敗時，會自動恢復（根據配置有一定次數）並無縫完成應用程序|
    |延遲計算（Lazy Evolution）|RDD 轉換不會在遇到 Driver 時被評估；而是在遇到 DAG 且 DAG 看到第一個 RDD 動作時才會對所有轉換進行評估|
    |分區（Partitioning）|當從資料創建 RDD，Default 會將 RRD 中的元素根據可用核心數分區|
  * 限制：不適合做狀態存儲更新，如 Web 應用程序的存儲系統，用傳統 DB 會更適合，因為 RDD 的目標是為批處理分析提供一個高效的編程模型，並留下這些異步應用程序
