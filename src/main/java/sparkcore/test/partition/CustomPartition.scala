package sparkcore.test.partition

import org.apache.spark.Partitioner

class CustomPartition(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
      if(key.toString.equals("first")){
        0
      }else{
        1
      }
  }
  override def equals(first: Any): Boolean = first match {
    case test:CustomPartition  =>test.numPartitions == numPartitions
    case _ =>
      false
  }
}
