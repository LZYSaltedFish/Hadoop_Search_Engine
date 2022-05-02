import java.io.IOException;
import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex 
{

    /**
     * Hadoop的mapreduce读取数据是一行一行来读取的，然后返回kv键值对
     * KEYIN: 表示map阶段输入kv中的k类型，表示每一行起始位置的偏移量，通常来讲没有意义
     * VALUEIN: 表示map阶段输入kv中的v类型，表示的是这一行的文本内容
     * KEYOUT: 表示map阶段输出kv中的k类型
     * VALUEOUT: 表示map阶段输出kv中的v类型
     */
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private Text keyInfo = new Text();       // 存储单词和url的组合，组合的形式为：word;url
        private Text valueInfo = new Text();     // 存储词频，这里把词频也设置成了Text类型

        /**
         * map方法被调用的次数和输入的kv键值对有关，每当TextInputFormat读取返回一个kv键值对，就调用一次map方法进行业务处理
         * 默认情况下，map方法是基于行来处理数据的
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            JSONObject json = new JSONObject(line);
            String title = json.get("title").toString();
            String url = json.get("url").toString();
            String words[] = title.split("\\s+");
            for (String word : words)
            {
                keyInfo.set(word + ";" + url);
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);   // 本阶段map的输出形式为：<word;url , "1">
            }
        }
    }

    /**
     * KEYIN: 表示的是reduce阶段输入kv中k的类型，对应着map阶段输出的key
     * VALUEIN: 表示的是reduce阶段输入kv中v的类型，对应着map阶段输出的value
     * KEYOUT: 输出的key类型
     * VALUEOUT: 输出的value类型
     */
    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>
    {
        private Text info = new Text();    // 用于存储在该阶段输出的kv对中的value值

        /** 
         * 这里以wordcount业务举例说明
         * todo Question: 当map阶段所有输出数据来到reduce之后，该如何调用reduce方法进行处理？
         *       <hello, 1><hadoop, 1><hello, 1><hello, 1><hadoop, 1>
         * Hadoop在调用reduce方法之前会在内部做以下三件事情：
         * 1、排序  规则：根据key的字典序进行排序
         *       <hadoop, 1><hadoop, 1><hello, 1><hello, 1><hello, 1>
         * 2、分组  规则：key相同的分为一组
         *       <hadoop, 1><hadoop, 1>
         *       <hello, 1><hello, 1><hello, 1>
         * 3、分组之后，同一组的数据组成一个新的kv键值对，调用一次reduce（因此，reduce方法基于分组调用，一个分组调用一次）
         *       todo 同一组中数据组成一个新的kv键值对
         *       新key：该组共同的key
         *       新value：该组所有的value组成的一个迭代器Iterable
         *       <hadoop, 1><hadoop, 1> --------> <hadoop, Iterable[1, 1]>
         *       <hello, 1><hello, 1><hello, 1> ------> <hello, Iterable[1, 1, 1]>
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            int wordCount = 0;
            for (Text value : values) 
            {
                wordCount += Integer.parseInt(value.toString());    
            }

            int splitIndex = key.toString().indexOf(";");   // reduce阶段输入key的形式为：word;url，因此该行代码用来找到分隔符“;”所在的索引
            key.set(key.toString().substring(0, splitIndex));  // 重新设置key为word
            info.set(key.toString().substring(splitIndex+1) + ":" + wordCount);
            context.write(key, info);    // 本reduce阶段的输出形式为：<word, url:wordCount>
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {
        private Text finalValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            String urlList = new String();
            for (Text value : values) 
            {
                urlList += value.toString() + ";";
            }
            finalValue.set(urlList);
            context.write(key, finalValue);
        }
    }

    /**
     * @description: 该main方法就是MapReduce程序客户端的驱动程序，主要是构造Job对象实例
     *               指定各种组件属性，包括：mapper reducer类、输入输出的数据类型、输入输出的数据路径
     *               提交Job作业 job.submit()
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception 
    {
        // 创建配置对象
        Configuration conf = new Configuration();

        // 构建Job作业的实例 参数（配置对象、Job的名字）
        Job job = Job.getInstance(conf, InvertedIndex.class.getSimpleName());

        // 设置MapReduce程序运行的主类
        job.setJarByClass(InvertedIndex.class);

        // 设置本次MapReduce程序的mapper类 combiner类 reducer类
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // 指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 指定reducer阶段输出的key value类型，也是MapReduce程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 配置本次作业的输入数据路径和输出数据路径
        // todo 默认组件：TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 最终提交本次job作业
        // 采用waitForCompletion提交job，参数表示是否开启实时监视追踪作业的执行情况
        boolean flag = job.waitForCompletion(true);

        // 退出程序，并和job结果进行绑定
        System.exit(flag ? 0 : 1);
    }
}
