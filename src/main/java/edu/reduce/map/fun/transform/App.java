package edu.reduce.map.fun.transform;

import edu.reduce.map.fun.GameInputFormat;
import edu.reduce.map.fun.elo.EloMapper;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroOutputFormatBase;
import org.apache.commons.collections.map.IdentityMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.avro.AvroSchemaConverter;

import java.io.IOException;

public class App {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("coucou");
        final Job job = configureJob("funWithMapReduce");
        GameInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        System.out.println(loadSchema().toString());
    }

    private static Schema loadSchema() throws IOException {
        return new Schema.Parser().parse(App.class.getClassLoader().getResourceAsStream("avro/chess.avsc"));
    }

    private static Job configureJob(final String jobName) throws IOException {
        Job job = Job.getInstance(new Configuration(), jobName);
        job.setInputFormatClass(GameInputFormat.class);

        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(AvroValue.class);
        job.setOutputFormatClass(AvroOutputFormatBase.class);

        Schema schema = loadSchema();

        System.out.println("Schema : " + new AvroSchemaConverter().convert(schema).toString());


        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.LONG));
        AvroJob.setOutputValueSchema(job, schema);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);

        job.setMapperClass(AvroMap.class);
        job.setReducerClass(AvroReducer.class);

        job.setJarByClass(AvroMap.class);

        return job;
    }
}
