import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.ArrayList;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TopTitles extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopTitles(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/question_3_2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Carrier Arrive On Time Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapperClass(TitleCountMap.class);
        jobA.setReducerClass(TitleCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopTitles.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Carrier");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(TopTitlesMap.class);
        jobB.setReducerClass(TopTitlesReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopTitles.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class TitleCountMap extends Mapper<Object, Text, Text, Text> {
        String delimiters;
        SimpleDateFormat formatter;
        Calendar cal;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.delimiters = new String(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            formatter = new SimpleDateFormat("yyyy-MM-dd");
            this.cal = Calendar.getInstance();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
	    String[] data = line.split(this.delimiters);
            try{
               // if (data.length == 7) {
                    Date date = formatter.parse(data[0]);
		    context.write(new Text(formatter.format(date)), new Text(data[0] + "," + data[1] + "," + data[2] + "," + data[3] + "," + data[4] + "," + data[5] + "," + data[6]));
		    Calendar cal = Calendar.getInstance();
		    cal.setTime(date);
                    cal.add(Calendar.DATE, -2);
		    Date dateBefore2Days = cal.getTime();
                    context.write(new Text(formatter.format(dateBefore2Days)), new Text(data[0] + "," + data[1] + "," + data[2] + "," + data[3] + "," + data[4] + "," + data[5] + "," + data[6]));
                    //}
                //}
            } catch (Exception e) {
	    }
        }
    }

    public static class TitleCountReduce extends Reducer<Text, Text, Text, Text> {
        String delimiters;
        SimpleDateFormat formatter;
        Calendar cal;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.delimiters = new String(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            formatter = new SimpleDateFormat("yyyy-MM-dd");
            this.cal = Calendar.getInstance();
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> xlist = new ArrayList<Text> ();
            List<Text> ylist = new ArrayList<Text> ();

           for (Text val : values) {
                xlist.add(val);
                ylist.add(val);
           }

            for (Text x : xlist) {
                String[] xdata = x.toString().split(this.delimiters);
                Text t = new Text ("hola");
                for (Text y : xlist) {
                    String[] ydata = y.toString().split(this.delimiters);
                    try{
                        Date ydate = formatter.parse(ydata[0]);
                        Date xdate = formatter.parse(xdata[0]);

                        if (xdate.before(ydate)) {
                            if (xdata[3] == ydata[2]) {
                                if (Integer.parseInt(xdata[4]) < 1200 && Integer.parseInt(ydata[4]) > 1200) {
                                    context.write(new Text(xdata[0] + "," +  xdata[2] + "," +  xdata[3] + "," + ydata[3]), new Text(xdata[1] + "," + xdata[5] + "," + xdata[6] + "," +  ydata[1] + "," + ydata[5] + "," + ydata[6]));
                                }
                            }
                        }
                        else if (ydate.before(xdate)) {
                           if (ydata[3] == xdata[2]) {
                               if (Integer.parseInt(ydata[4]) < 1200 && Integer.parseInt(xdata[4]) > 1200) {
                                   context.write(new Text(ydata[0] + "," +  ydata[2] + "," +  ydata[3] + "," + xdata[3]), new Text(ydata[1] + "," + ydata[5] + "," + ydata[6] + "," +  xdata[1] + "," + xdata[5] + "," + xdata[6]));
                               }
                           }
                       }
                    } catch (Exception e) {
		    }
                }
            }
        }
    }

    public static class TopTitlesMap extends Mapper<Text, Text, Text, Text> {
        String delimiters;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.delimiters = new String(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }

    }

    public static class TopTitlesReduce extends Reducer<Text, Text, Text, Text> {
        String delimiters;
	Integer N;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.delimiters = new String(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	    TreeSet<Pair<Double, String>> XY = new TreeSet<Pair<Double, String>>();
	    TreeSet<Pair<Double, String>> YZ = new TreeSet<Pair<Double, String>>();

	    for (Text val : values) {
                String[] data = val.toString().split(this.delimiters);
                XY.add(new Pair<Double, String>(Double.parseDouble(data[1]),data[2]));
                YZ.add(new Pair<Double, String>(Double.parseDouble(data[4]),data[5]));
            }

            context.write(key, new Text (XY.last().first.toString() + YZ.last().first.toString()));

        }

    }

}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
