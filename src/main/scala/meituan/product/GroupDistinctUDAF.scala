package meituan.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

object GroupDistinctUDAF extends UserDefinedAggregateFunction { //UDAF 用户自定义函数
	/**
		* 输入的数据类型  1:beijing
		*
		* @return
		*/
	override def inputSchema: StructType = StructType(
		StructField("city_info", StringType, true) :: Nil
	)

	/**
		*
		* @return
		*/
	override def dataType: DataType = StringType


	/**
		* 定义辅助字段
		*
		* @return
		*/
	override def bufferSchema: StructType = {
		bufferSchema.add("buffer_city_info", StringType, true)
	}


	/**
		* 初始化辅助字段
		*
		* @param buffer
		*/
	override def initialize(buffer: MutableAggregationBuffer): Unit = {
		buffer.update(0, "") //0 代表的是初始化字段的位置
	}

	/**
		* 局部操作
		*
		* @param buffer 上一次缓存的结果
		* @param input  input 这一次的输入
		*/
	override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
		var buffer_str = buffer.getString(0)
		val current_str = input.getString(0)

		/**
			* 去重操作  如果上一次缓存的结果不包含当前输入的结果，而且上一次缓存的结果为空
			* 那么就将当前输入的结果写入，否则就在上次缓存的结果之后进行追加当前输入的结果
			**/
		if (!buffer_str.contains(current_str)) {
			if (buffer_str.eq("")) {
				buffer_str = current_str
			} else {
				buffer_str + "," + current_str
			}
		}
		buffer.update(0, buffer_str) //对结果进行更新
	}

	/**
		* 进行全局汇总
		*
		* @param buffer1
		* @param buffer2
		*/
	override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
		var b1 = buffer1.getString(0)
		val b2 = buffer2.getString(0)
		for (b <- b2.split(",")) {
			if (!b1.contains(b)) {
				if (b1.equals("")) {
					b1 = b
				} else {
					b1 + "," + b
				}

			}
		}
		b1.updated(0, b1)

	}

	/**
		* 拿到最后的结果
		*
		* @param buffer
		* @return
		*/

	override def evaluate(buffer: Row): Any = {
		buffer.getString(0) 
	}


	override def deterministic: Boolean = true

}
