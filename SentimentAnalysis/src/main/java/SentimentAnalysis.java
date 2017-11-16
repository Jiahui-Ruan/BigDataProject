import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SentimentAnalysis {

    public static class SentimentSplit extends Mapper<Object, Text, Text, IntWritable> {
        public Map<String, String> emotionLibrary = new HashMap<String, String>();

        @Override
        public void setup(Context context) throws IOException{
            // init emotionLibrary
            Configuration configuration = context.getConfiguration();
            String path = configuration.get("dictionary");
            BufferedReader br = new BufferedReader(new FileReader(path));
            String line = br.readLine();

            while (line != null) {
                String[] word_feeling = line.split("\t");
                emotionLibrary.put(word_feeling[0], word_feeling[1]);
                line = br.readLine();
            }

            br.close();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // key = offset
            // value = line
            // read file
            // split into single words
            // look up emotionLibrary to find emotion
            // write k-v pair
            String[] words = value.toString().split("\\s+"); // split by space
            for (String word : words) {
                if (emotionLibrary.containsKey(word)) {
                    String sentiment = emotionLibrary.get(word.toLowerCase());
                    context.write(new Text(sentiment), new IntWritable(1));
                }
            }
        }
    }

    public static class SentimentCollection extends Reducer<Text, IntWritable, Text, IntWritable> {
        Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            // key = positive
            // value = <1, 1, 1, 1>
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

    }
}
