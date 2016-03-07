package hello;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.replication.SystemTableWALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by Sean on 2/21/2016.
 */
public class QueryNumSpeedsOver100 {
    public static void main(String[] args) {
        try {

            Configuration config = HBaseConfiguration.create();
            Job job = Job.getInstance(config, "PageViewCounts");
            job.setJarByClass(QueryNumSpeedsOver100.class);     // class that contains mapper and reducer

            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs
            // set other scan attrs

            TableMapReduceUtil.initTableMapperJob(
                    "freeway_loopdata_full",        // input table
                    scan,               // Scan instance to control CF and attribute selection
                    MyMapper1.class,     // mapper class
                    Text.class,         // mapper output key
                    IntWritable.class,  // mapper output value
                    job);
            TableMapReduceUtil.initTableReducerJob(
                    "results",        // output table
                    MyTableReducer.class,    // reducer class
                    job);
            job.setNumReduceTasks(1);// at least one, adjust as required

            TableMapReduceUtil.addDependencyJars(job);

            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class MyMapper1 extends TableMapper<Text, IntWritable> {
        public static final byte[] CF = "freeway_loopdata".getBytes();
        public static final byte[] ATTR1 = "speed".getBytes();
        public static final byte[] ATTR2 = "starttime".getBytes();

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

            try {
                Query1(value, context);
            } catch (ParseException e) {
                //do nothing - invalid row
            }

        }

        private void Query1(Result value, Context context) throws IOException, InterruptedException, ParseException{
            String val = Bytes.toString(value.getValue(CF, ATTR1));
            String startTime = Bytes.toString(value.getValue(CF, ATTR2));

            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            Date time = format.parse(startTime);


            if (!val.isEmpty() && !val.equals("speed")) {
                Integer speed = Integer.parseInt(val);

                if (speed > 50) {
                    text.set("Speeds over 50:");
                    context.write(text, ONE);
                }
                if (speed > 60) {
                    text.set("Speeds over 60:");
                    context.write(text, ONE);
                }
                if (speed > 70) {
                    text.set("Speeds over 70:");
                    context.write(text, ONE);
                }
                if (speed > 80) {
                    text.set("Speeds over 80:");
                    context.write(text, ONE);
                }
                if (speed > 90) {
                    text.set("Speeds over 90:");
                    context.write(text, ONE);
                }
                if (speed > 100) {
                    text.set("Speeds over 100:");
                    context.write(text, ONE);
                }
                if (speed > 102) {
                    text.set("Speeds over 102:");
                    context.write(text, ONE);
                }
                if (speed > 104) {
                    text.set("Speeds over 104:");
                    context.write(text, ONE);
                }
                if (speed > 106) {
                    text.set("Speeds over 106:");
                    context.write(text, ONE);
                }
                if (speed > 108) {
                    text.set("Speeds over 108:");
                    context.write(text, ONE);
                }
                    text.set("Total Speed:");
                    context.write(text, new IntWritable(speed));
                    text.set("Total speed records");
                    context.write(text, ONE);
            }
        }
    }

    public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public static final byte[] CF = "results".getBytes();
        public static final byte[] COUNT = "count".getBytes();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }

            Integer iv = i;
            String val = iv.toString();
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(CF, COUNT, Bytes.toBytes(val));

            context.write(null, put);
        }
    }
}

