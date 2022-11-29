package SparkInAction

import org.apache.spark.sql.SparkSession

object SparkCheckpoint extends App {

  val spark = SparkSession
    .builder
    .master("local")
    .appName("SparkCheckpoint")
    .getOrCreate()

  val data = Seq(("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL"),
    ("James", null, "USA", "CA"),
    ("James", "Smith", "USA", "CA"),
    ("James", "Smith", null, "CA")
  )
  val columns = Seq("firstname", "lastname", "country", "state")

  import spark.implicits._

  val df = data.toDF(columns: _*)
  df.show()

  /*
  Checkpoint:
  Eagerly checkpoint a Dataset and return the new Dataset. Checkpointing can
  be used to truncate the logical plan of this Dataset, which is especially
  useful in iterative algorithms where the plan may grow exponentially. It
  will be saved to files inside the checkpoint directory set with SparkContext#setCheckpointDir.

  Persist / cache keeps lineage intact while checkpoint breaks lineage.

  lineage is preserved even if data is fetched from the cache. It means that
  data can be recomputed from scratch if some partitions of indCache are lost.
  In the second case lineage is completely lost after the checkpoint and indChk doesn't
  carry an information required to rebuild it anymore.

  checkpoint, unlike cache / persist is computed separately from other jobs. That's why RDD
  marked for checkpointing should be cached:

  1. The checkpoint file won't be deleted even after the Spark application terminated.
  2. Checkpoint files can be used in subsequent job run or driver program
  3. Checkpointing an RDD causes double computation because the operation will first
  call a cache before doing the actual job of computing and writing to the
  checkpoint directory.

  When to checkpoint ?
  As mentioned above, every time a computed partition needs to be cached, it
  is cached into memory. However, checkpoint does not follow the same principle.
  Instead, it waits until the end of a job, and launches another job to finish
  checkpoint. An RDD which needs to be checkpointed will be computed twice; thus
  it is suggested to do a rdd.cache() before rdd.checkpoint(). In this case, the
  second job will not recompute the RDD. Instead, it will just read cache. In fact,
  Spark offers rdd.persist(StorageLevel.DISK_ONLY) method, like caching on disk.
  Thus, it caches RDD on disk during its first computation, but this kind of persist
  and checkpoint are different, we will discuss the difference later

  Two RDDs are checkpointed in driver program, only the downstream RDD will be intentionally checkpointed.

  What is the difference between cache and checkpoint ?

  Here is the an answer from Tathagata Das:
  There is a significant difference between cache and checkpoint. Cache materializes
  the RDD and keeps it in memory (and/or disk). But the lineage（computing chain）
  of RDD (that is, seq of operations that generated the RDD) will be remembered,
  so that if there are node failures and parts of the cached RDDs are lost, they
  can be regenerated. However, checkpoint saves the RDD to an HDFS file and actually
  forgets the lineage completely. This allows long lineages to be truncated and
  the data to be saved reliably in HDFS, which is naturally fault tolerant by replication.

  Furthermore, rdd.persist(StorageLevel.DISK_ONLY) is also different from checkpoint.
  Through the former can persist RDD partitions to disk, the partitions are managed
  by blockManager. Once driver program finishes, which means the thread where CoarseGrainedExecutorBackend
  lies in stops, blockManager will stop, the RDD cached to disk will be dropped (local
  files used by blockManager will be deleted). But checkpoint will persist RDD to
  HDFS or local directory. If not removed manually, they will always be on disk,
  so they can be used by the next driver program.

  Points to remember:
  When Hadoop MapReduce executes a job, it keeps persisting data (writing to HDFS) at
  the end of every task and every job. When executing a task, it keeps swapping between
  memory and disk, back and forth. The problem of Hadoop is that task needs to be
  re-executed if any error occurs, e.g. shuffle stopped by errors will have only half of
  the data persisted on disk, and then the persisted data will be recomputed for the next
  run of shuffle. Spark's advantage is that, when error occurs, the next run will read
  data from checkpoint, but the downside is that checkpoint needs to execute the job twice.
   */
  spark.sparkContext.setCheckpointDir("D:\\tmp\\checkpoint")
  df.checkpoint()

}
