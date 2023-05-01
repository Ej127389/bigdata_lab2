package mapreduce;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Objects;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyTFIDF {


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

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {
        private Text result = new Text();
        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            String fileList = new String();
            int filecnt = 0;//包含该词的文档数
            //int sum = 0;
            for (Text value : values) {
                filecnt = filecnt + 1;
                int index = value.toString().indexOf(":");
                //sum += Integer.parseInt(value.toString().substring(index + 1));
                //fileList += value.toString() + ";";
                int tf = Integer.parseInt(value.toString().substring(index + 1));
                String str = context.getConfiguration().get("name");
                double n = Double.valueOf(str);
                double idf = Math.log((double) (n) / ((double) (filecnt) + 1));
                double tf_idf = tf * idf;
                String filename = value.toString().substring(0, index);
                result.set(filename + "," + key + "," + String.format("%.2f",tf_idf));
                context.write(result, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);
        job.setJarByClass(MyTFIDF.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(args[0]));

        // 设置输入和输出目录
        Path input_path = new Path(args[0]);
        Path output_path = new Path(args[1]);
        //获取文件个数/语料库文档总数
        //File folder = new File(args[0]);
        //int filenum = 0;
        //filenum = ;
        job.getConfiguration().set("name",String.valueOf(status.length));

        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


