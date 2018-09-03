import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Brave
 */
object SecondarySortDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Secondary Sort")
    val sc = new SparkContext(conf)

    val personRDD = sc.textFile("names.csv")
    val pairsRDD = personRDD.map(_.split(",")).map { k => (k(0), k(1)) }

    val numReducers = 2;

    val listRDD = pairsRDD.groupByKey(numReducers).mapValues(iter => iter.toList.sortBy(r => r))

    val resultRDD = listRDD.flatMap {
      case (label, list) => {
        list.map((label, _))
      }
    }
    
    resultRDD.foreach(t=>println(t._1+","+t._2))
//    val hadoopConf = new org.apache.hadoop.conf.Configuration()
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://quickstart.cloudera:8020"), hadoopConf)
//    try {
//      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
//    } catch {
//      case _: Throwable => {}
//    }
//
//    resultRDD.saveAsTextFile(args(1))

  }
}