import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class TSVParse {

    private static final Schema MAPPING_SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "    \"type\":    \"record\",\n" +
                    "    \"name\":    \"dhcp\",\n" +
                    "    \"fields\":\n" +
                    "    [\n" +
                    "            {\"name\":    \"line\", \"type\":    \"string\"}\n"+
                    "    ]\n"+
                    "}\n");

    public static class TSVParserMapper extends Mapper<LongWritable, Text, Void, GenericRecord>{

        private GenericRecord record = new GenericData.Record(MAPPING_SCHEMA);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            record.put("line", value.toString());
            context.write(null,record);
        }

    }
    public static class ConfParser{

        public static String inputDir, outputDir, outputFileName;
        public static JSONArray fields;

        public ConfParser(String path){   //path to conf.json

            JSONParser jsonParser = new JSONParser();
            try(FileReader reader = new FileReader(path)){
                Object obj = jsonParser.parse(reader);

                JSONObject file = (JSONObject) obj;
                ConfParser.inputDir = file.get("inputDir").toString();
                ConfParser.outputDir = file.get("outputDir").toString();
                ConfParser.outputFileName = file.get("outputFileName").toString();
                ConfParser.fields = (JSONArray) file.get("fields");


            }
            catch(ParseException e){
                e.printStackTrace();
            }
            catch(FileNotFoundException e){
                e.printStackTrace();
            }
            catch(IOException e){
                e.printStackTrace();
            }

        }

    }
    public static void main(String args[]) throws Exception{
        try{
            Job job = Job.getInstance(new Configuration(), "TSVParse");
            job.setJarByClass(TSVParse.class);
            job.setMapperClass(TSVParserMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Void.class);
            job.setOutputValueClass(Group.class);
            job.setOutputFormatClass(AvroParquetOutputFormat.class);

            AvroParquetOutputFormat.setSchema(job, MAPPING_SCHEMA);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.out.println("TSV Parser");
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

}
