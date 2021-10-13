import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}

import java.lang

object WordCount extends Configured with Tool {

  val INPUT_PATH = "wordcount.in"
  val OUTPUT_PATH = "wordcount.out"

  override def run(args: Array[String]): Int = {
    val job = Job.getInstance(getConf, "Word count")
    job.setJarByClass(getClass)

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])
    job.setNumReduceTasks(2)

    val in = new Path(getConf.get(INPUT_PATH))
    val out = new Path(getConf.get(OUTPUT_PATH))

    FileInputFormat.setInputPaths(job, in)
    FileOutputFormat.setOutputPath(job, out)

    val fs = FileSystem.get(getConf)
    if (fs.exists(out)) {
      fs.delete(out, true)
    }

    if (job.waitForCompletion(true)) 0 else 1

  }

  def main(args: Array[String]): Unit = {
    val res: Int = ToolRunner.run(new Configuration(), this, args)
    System.exit(res)
  }
}

class MyMapper extends Mapper[Object, Text, Text, IntWritable] {

  val text = new Text()
  val one = new IntWritable(1)

  override def map(key: Object, value: Text,
                   context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val kv = value.toString.toLowerCase
    val words = kv.split("\\s")
    words.foreach(word => {
      if (!StringUtils.isEmpty(word)) {
        text.set(word)
        context.write(text, one)
      }
    })
  }
}

class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {

  val resultSum = new IntWritable(0)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

    var sum = 0
    values.forEach(currentValue => sum += currentValue.get())

    resultSum.set(sum)
    context.write(key, resultSum)
  }
}
