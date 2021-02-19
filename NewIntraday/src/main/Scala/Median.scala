import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 *
 * 自定义UDAF函数取一列Float类型数据的中位数
 *
 */
class Median extends UserDefinedAggregateFunction {

  //聚合函数的输入数据结构
  override def inputSchema: StructType =
    StructType(StructField("value",FloatType)::Nil)

  //缓冲区的数据结构
  override def bufferSchema: StructType =
    StructType(StructField("data_list",ArrayType(FloatType, false))::Nil)

  //返回值的数据类型
  override def dataType: DataType =FloatType


  //是否是一个确定性的函数
  override def deterministic: Boolean = true

  //初始化缓冲区
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new ArrayBuffer[Float]()
  }

  //传入一条数据的时候的计算逻辑
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferVal = buffer.getAs[mutable.WrappedArray[Float]](0).toBuffer
    bufferVal += input.getAs[Float](0)
    buffer(0) = bufferVal
  }

  //合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[mutable.WrappedArray[Float]](0) ++ buffer2.getAs[mutable.WrappedArray[Float]](0)
  }


  override def evaluate(buffer: Row): Any = {

    val sortedWindow = buffer.getAs[mutable.WrappedArray[Float]](0).sorted.toBuffer
    val windowSize = sortedWindow.size
    if (windowSize % 2 == 0) {
      val index = windowSize / 2
      (sortedWindow(index) + sortedWindow(index - 1)) / 2
    } else {
      sortedWindow((windowSize + 1) / 2 - 1)
    }


  }
}
