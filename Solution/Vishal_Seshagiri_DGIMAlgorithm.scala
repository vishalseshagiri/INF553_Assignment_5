import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.math.pow
import scala.collection.immutable.ListMap
import org.apache.spark.util.AccumulatorV2



object DGIMAlgorithm {
  var most_recent_1000 = ListBuffer[Integer]()
  var dgim_indices = mutable.HashMap[Integer, ListBuffer[Integer]]()

  def count_words(stream : RDD[String]): Unit ={
    val bitstream = ListBuffer[Integer]()
    for(i <- stream.collect()) {
      bitstream += i.toInt
    }

    if (most_recent_1000.size < 1000){
      most_recent_1000 ++= bitstream
    } else {
      most_recent_1000 ++= bitstream
      most_recent_1000 = most_recent_1000.slice(most_recent_1000.size-999, most_recent_1000.size)
    }

    val number_of_bits_in_current_batch = bitstream.size

    for (key <- dgim_indices.keys.toList.sorted) {
      var tempListBuffer = new ListBuffer[Integer]()
      var value_option = dgim_indices.get(key)
      for (i <- value_option.getOrElse(ListBuffer[Integer]())) {
        if (i + number_of_bits_in_current_batch < 1000) {
          tempListBuffer += i + number_of_bits_in_current_batch
        }
      }
      dgim_indices(key) = tempListBuffer
    }

    for ((bit, bit_position) <- bitstream.zipWithIndex) {
      var new_bit_position = number_of_bits_in_current_batch - 1 - bit_position
      if (bit == 1) {
        if (dgim_indices.keys.toList.contains(1)) {
          dgim_indices(1).append(new_bit_position)
        } else {
          dgim_indices(1) = ListBuffer(new_bit_position)
        }
        for (index <- dgim_indices.keys.toList.sorted) {
          var current_vals = dgim_indices.getOrElse(index, ListBuffer())
          if (current_vals.size > 2) {
            dgim_indices.getOrElseUpdate(index + 1, ListBuffer())
            dgim_indices(index + 1) += current_vals(1)
            dgim_indices(index) = ListBuffer(current_vals(2))
          }
        }
      }
    }
    var estimated_count = 0
    if (dgim_indices.keys.toList.nonEmpty){
      var keys = dgim_indices.keys.toList.sorted
      for (key <- keys.slice(1, keys.size-1)) {
        estimated_count += pow(2, key-1).toInt * dgim_indices.getOrElse(key, ListBuffer()).size
      }

      var key = keys.size

      if(dgim_indices.getOrElse(key, ListBuffer()).size > 1) {
        estimated_count += pow(2, key - 1).toInt + pow(2, key - 1).toInt / 2
      } else {
        estimated_count += pow(2, key - 1).toInt / 2
      }

      println(s"Estimated number of ones in the last 1000 bits: ${estimated_count}")
      println(s"Actual number of ones in the last 1000 bits: ${most_recent_1000.count(_ == 1)}")
      println
      println
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("DGIMAlgorithm").setMaster("local[2]")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val stream = ssc.socketTextStream("localhost", 9999)
    stream.foreachRDD(rdd => count_words(rdd))

    ssc.start()
    ssc.awaitTermination()
  }

}