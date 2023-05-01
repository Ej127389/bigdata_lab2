package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class MyIndex {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private FileSplit split; // 存储Split对象
        // 实现map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // key值由单词和URL组成，如"MapReduce：file1.txt"
                // 获取文件的完整路径
                // keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
                // 这里为了好看，只获取文件的名称。
                int splitIndex = split.getPath().toString().indexOf("第");
                keyInfo.set("[" + itr.nextToken() + "]" + ":" + split.getPath().toString().substring(splitIndex));
                // 词频初始化为1
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 统计词频
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            int splitIndex = key.toString().indexOf(":");
            // 重新设置value值由URL和词频组成
            info.set(key.toString().substring(splitIndex + 1) + ":" + sum);
            // 重新设置key值为单词
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            String fileList = new String();
            int filecnt = 0;
            int sum = 0;
            for (Text value : values) {
                filecnt = filecnt + 1;
                int index = value.toString().indexOf(":");
                sum += Integer.parseInt(value.toString().substring(index + 1));
                fileList += value.toString() + ";";
            }
            double aveFreq = (double)(sum) / (double)(filecnt);
            fileList = String.format("%.2f",aveFreq) + "," + fileList;
            result.set(fileList);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);
        job.setJarByClass(MyIndex.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        //FileInputFormat.addInputPath(job, new Path("hdfs://master001:9000/data/exp2"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


