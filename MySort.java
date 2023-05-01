package mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MySort {

    public static class SortUnit implements WritableComparable<SortUnit> {
        private double aveg;
        private String wordname;
        public SortUnit(){

        }
        public double getaveg() {
            return aveg;
        }

        public void setaveg(double aveg) {
            this.aveg = aveg;
        }

        public String getwordname() {
            return wordname;
        }

        public void setwordname(String wordname) {
            this.wordname = wordname;
        }
        @Override
        public String toString() {
            return wordname + "\t" + String.format("%.2f",aveg);
        }
        @Override
        public int compareTo(SortUnit o) {
            if(this.aveg < o.getaveg())
                return 1;
            else if(this.aveg > o.getaveg())
                return -1;
            else {
                return this.wordname.compareTo(o.getwordname());
            }

        }
        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(this.aveg);
            dataOutput.writeUTF(this.wordname);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.aveg = dataInput.readDouble();
            this.wordname = dataInput.readUTF();
        }
    }

    public int compare(SortUnit a, SortUnit b) {
        System.out.println("Hello World");
        return a.compareTo(b);
    }
    public static class Map extends Mapper<Object, Text, SortUnit, NullWritable> {
        private SortUnit keyInfo = new SortUnit();
        private FileSplit split; // 存储Split对象
        // 实现map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            String line = value.toString();
            String[] fields = line.split("\t");
            keyInfo.setwordname(fields[0]);
            int index = fields[1].indexOf(",");
            keyInfo.setaveg(Double.valueOf(fields[1].substring(0,index)));
            context.write(keyInfo,NullWritable.get());
        }

    }

    public static class Reduce extends Reducer<SortUnit, NullWritable, SortUnit, NullWritable> {
        //private Text result = new Text();
        //private SortUnit u = new SortUnit();
        // 实现reduce函数
        public void reduce(SortUnit key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // 生成文档列表
            //String fileList = new String();

            context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(MySort.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(SortUnit.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(SortUnit.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


