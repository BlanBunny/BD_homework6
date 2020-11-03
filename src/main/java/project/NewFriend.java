package project;

import java.util.StringTokenizer;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NewFriend {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{    //改成Text试试

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text firstword=new Text();    //每一行第一个单词

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                //通过判断单词是否含有逗号判断是否为这一行第一个单词
                String regEx=",";
                Pattern p=Pattern.compile(regEx);
                Matcher m=p.matcher(word.toString());
                if(m.find())    //在单词中找到了逗号，说明是这一行第一个单词;这一行<key,value>不存进去
                {
                    String s=word.toString();
                    String s1=s.replaceAll(regEx,"");   //先把逗号去掉
                    word=new Text(s1);
                    firstword.set(word);    //确定当前单词为firstword
                }
                else
                    context.write(word,firstword);
            }
        }
    }

    public static class PartReducer
            extends Reducer<Text,Text,Text,Text> {
        //private IntWritable result = new IntWritable();
        private int count=0;
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
           StringBuilder tmp= new StringBuilder();

           for(Text val : values)
           {
               if(count>0)
                   tmp.append(",");
               tmp.append(val);
               count++;
           }

            context.write(key,new Text(tmp.toString()));
        }
    }






    public static class sortJobMapper
            extends Mapper<Object, Text, Text, Text>{

        //private IntWritable result = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            //line=line.replaceAll(" ","");
            String[] mix = line.split(",");
            Arrays.sort(mix);     //保证不重复
            if (mix.length >= 2) {
                for (int i = 0; i < mix.length - 1; i++) {
                    for (int j = i + 1; j < mix.length ; j++) {
                        String res = mix[i] + ", " + mix[j];    //合并成key
                        context.write(new Text(res), new Text(key.toString()));
                    }
                }
            }
        }
    }




    //新的job的reduce
    public static class AutoReducer extends Reducer<Text,Text,Text, NullWritable>{

        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
            StringBuilder friend= new StringBuilder();     //存储共同好友
            int count=0;
            for(Text val:values) {
                if(count>0)
                    friend.append(", ");
                friend.append(val.toString());
                count++;
            }
            String str= key.toString();
            char fir = str.charAt(0);
            if(fir!=',') {

                String res = "(" + "[" + key.toString() + "]" + ", " + "[" + friend + "]" + ")";
                context.write(new Text(res), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (remainingArgs.length < 2) {
            System.err.println("Usage: newfriend <in> [<in>...] <out>");
            System.exit(2);
        }


        // 定义一个临时目录
        Path tempDir = new Path("NewFriend-temp-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));  //new

        Job job = Job.getInstance(conf,"new friend");
        job.setJarByClass(NewFriend.class);         //
        job.setMapperClass(TokenizerMapper.class);            //Mapper
        //job.setCombinerClass(PartReducer.class);            //Combine  词频降序操作
        job.setReducerClass(PartReducer.class);             //Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

 //       job.setInputFormatClass(CombineFileInputFormat.class);
//        CombineFileInputFormat.setMaxInputSplitSize(job,4194304);
//        CombineFileInputFormat.setMinInputSplitSize(job,2097152);
        /*
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

         */
        List<String> otherArgs = new ArrayList<String>();
        for(int i=0;i< remainingArgs.length;++i){
                otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job,new Path(otherArgs.get(0)));    //词频降序操作
        FileOutputFormat.setOutputPath(job,tempDir);

        job.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

        if (job.waitForCompletion(true))
        // 只有当job任务成功执行完成以后才开始sortJob，参数true表明打印verbose信息
        {
            Job sortJob = Job.getInstance(conf, "segma");

            sortJob.setJarByClass(NewFriend.class);
            FileInputFormat.addInputPath(sortJob, tempDir);
            sortJob.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
            sortJob.setMapperClass(sortJobMapper.class);
            sortJob.setReducerClass(AutoReducer.class);             //Reducer   自己添加的
            // InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换

            sortJob.setNumReduceTasks(1);
            // 将Reducer的个数限定为1，最终输出的结果文件就是一个

            FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(Text.class);

            /*
             * Hadoop默认对IntWritable按升序排序，而我们需要的是按降序排列。
             * 因此我们实现了一个IntWritableDecreasingCompatator类，并指定使用这个自定义的Comparator类，
             * 对输出结果中的key（词频）进行排序
             */
            System.exit(sortJob.waitForCompletion(true) ? 0 : 1);

        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}




