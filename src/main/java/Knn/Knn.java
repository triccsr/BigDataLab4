package Knn;

        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.io.IOException;
        import java.util.*;

        //import javafx.util.Pair;
        import Pair.Pair;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class Knn {
    private static final int K = 10;

    public static class KNNMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private ArrayList<double[]> testData=new ArrayList<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 将 distributed cache file 中的测试数据装入本地的内存数据 testData 中
            System.out.println("setup");
            try {
                Path[] cacheFiles = context.getLocalCacheFiles();
                System.out.println(cacheFiles);
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    String[] tokens;
                    double[] temp = new double[4];
                    BufferedReader joinReader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    try {
                        while ((line = joinReader.readLine()) != null) {
                            System.out.println(line);
                            tokens = line.split(",", 4);
                            for(int i = 0; i < 4; ++i)
                                temp[i] = Double.parseDouble(tokens[i]);
                            testData.add(temp.clone());
                            System.out.println("setup:");
                            System.out.println(Arrays.toString(tokens));
                            System.out.println(testData);
                        }
                    } finally {
                        joinReader.close();
                    }
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache: "+e);
            }
        }

        private double distance(double[] A, double[] B){
            double ret = 0;
            for(int i = 0; i < 4; ++i)
                ret += (A[i] - B[i]) * (A[i] - B[i]);
            return ret;
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("mapping");
            String[] tokens = value.toString().split(",", 5);
            double[] values = new double[4];
            for(int i = 0; i < 4; ++i)
                values[i] = Double.parseDouble(tokens[i]);
            int id = 0;
            // 枚举所有训练数据，并且写入
            // key = 测试数据id; value = <距离，类>
            for(double[] test : testData) {
                double dist = distance(values, test);
                //context.write(new IntWritable(id), new Pair<Float, String>(dist, tokens[4]));
                Text text=new Text(dist +"\t"+tokens[4]);
                context.write(new IntWritable(id),text);
                System.out.printf("map %d %s %s\n",id,tokens.toString(),text);
                id ++;
            }
        }
    }

    /*public static class KNNCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        Comparator<Text> textComparator=new Comparator<Text>() {
            @Override
            public int compare(Text o1, Text o2) {
                String[] sa=o1.toString().split("\t");
                String[] sb=o2.toString().split("\t");
                return Double.compare(Double.parseDouble(sa[0]),Double.parseDouble(sb[0]));
            }
        };
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            System.out.println("comb");
            ArrayList<Text> valArray=new ArrayList<>();
            for(Text val:values){
                System.out.println(val);
                valArray.add(val);
            }
            valArray.sort(textComparator);
            System.out.println(valArray);
            for(Text val : valArray){
                context.write(key, val);
                ++ count;
                if(count == K) break;
            }
        }
    }*/

    public static class KNNReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Comparator<Text> textComparator=new Comparator<Text>() {
                @Override
                public int compare(Text o1, Text o2) {
                    String[] sa=o1.toString().split("\t");
                    String[] sb=o2.toString().split("\t");
                    double a=Double.parseDouble(sa[0]),b=Double.parseDouble(sb[0]);
                    return Double.compare(a,b);
                }
            };

            ArrayList<Text> valArray=new ArrayList<>();
            System.out.printf("reduce %d:\n",key.get());
            for(Text val:values){
                System.out.println(val);
                valArray.add(new Text(val));
            }
            for(Text val:valArray)System.out.printf("%s ",val.toString());
            System.out.println();
            valArray.sort(textComparator);
            for(Text val:valArray)System.out.printf("%s ",val.toString());
            System.out.println();

            Map<String, Integer> m = new HashMap<String, Integer>();
            String mxType = "";
            int mxSum = 0;

            int count = 0;
            System.out.printf("sorted %d:\n",key.get());
            for(Text text : valArray){
                System.out.println(text);
                String[] s=text.toString().split("\t");
                Pair<Double,String> val=new Pair<>(Double.parseDouble(s[0]),s[1]);
                ++ count;
                String str = val.getValue();
                int temp = 1;
                if(m.containsKey(str)){
                    temp = m.get(str) + 1;
                    m.put(str, temp);
                } else
                    m.put(str, 1);
                if(temp > mxSum) {
                    mxType = str;
                    mxSum = temp;
                }
                if(count == K) break ;
            }
            context.write(key, new Text(mxType));
        }

    }

    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            //conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Job job = Job.getInstance(new Configuration(), "Knn");
            job.setJarByClass(Knn.class);

            job.setMapperClass(KNNMapper.class);
            //job.setMapOutputKeyClass(IntWritable.class);
            //job.setMapOutputValueClass(Pair.class);

            //job.setCombinerClass(KNNCombiner.class);
            job.setReducerClass(KNNReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            System.err.println(new Path(args[0]).toUri());
            job.addCacheFile(new Path(args[0]).toUri());
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
