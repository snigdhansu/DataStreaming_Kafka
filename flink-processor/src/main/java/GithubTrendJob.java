
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;


public class GithubTrendJob {
    static final String BROKERS = "kafka:9092";

    public static void main(String[] args) throws Exception {

        // Set the Streaming Environment 
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        System.out.println("Environment created");

        // Subscribe to the topic, thus creating the source of the streaming data and define the deserialization schema
        KafkaSource<GithubEventData> githubEventSource = KafkaSource.<GithubEventData>builder()
                                      .setBootstrapServers(BROKERS)
                                      .setProperty("partition.discovery.interval.ms", "1000")
                                      .setTopics("github-events")
                                      .setGroupId("groupdId-919292")
                                      .setStartingOffsets(OffsetsInitializer.earliest())
                                      .setValueOnlyDeserializer(new GithubEventDeserializationSchema())
                                      .build();
                        
        DataStreamSource<GithubEventData> githubEventStream = env.fromSource(githubEventSource, WatermarkStrategy.noWatermarks(), "kafka");

        System.out.println("Kafka source created");
        githubEventStream.print();
        
        // Aggregate the events based on repo name and count the events for each repo, similar to map-reduce algorithm 
        // Aggregation is calculated for every 2 minutes considering the last 10 minutes with the help of SlidingWindow
        DataStream<Tuple2<String, Integer>> repoAndValueStream = githubEventStream
                .map(event -> new Tuple2<>(event.getRepo().getName(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(2)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
                      return new Tuple2<>(v1.f0, v1.f1 + v2.f1);
                    }
                  });
                
        repoAndValueStream.print();
        System.out.println("Aggregation created");

        // Process the aggregated event count stream every 2 minutes, retrieve the top 10 trending repos
        DataStream<List<Tuple2<String, Integer>>> topTrendingRepos = repoAndValueStream
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .process(new MyProcessWindowFunction());
        // topTrendingRepos.print();

        
        // ----- Producer job

        // Create a JSON serializer
        SerializationSchema<Tuple2<String, Integer>> jsonSerializer = new JsonSerializationSchema<>(); 
        // Create the Kafka Sink
        KafkaSink<Tuple2<String, Integer>> githubSink = KafkaSink.<Tuple2<String, Integer>>builder()
                .setBootstrapServers(BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic("trending-github-repos")
                    .setValueSerializationSchema(jsonSerializer)
                    .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Send the top trending repos to another kafka cluster topic 
        topTrendingRepos
                .flatMap((List<Tuple2<String, Integer>> repos, Collector<Tuple2<String, Integer>> out) -> {
                        for (Tuple2<String, Integer> repo : repos) {
                            out.collect(repo);
                        }
                    })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .sinkTo(githubSink);
        
        // Execute the Flink job
        env.execute("Github-Trend-Job");
    }

    public static class MyProcessWindowFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> input, Collector<List<Tuple2<String, Integer>>> out) throws Exception {
            List<Tuple2<String, Integer>> repoList = new ArrayList<>();
            input.forEach(repoList::add);

            // Sort the list of repos, based on the count (every two minutes)
            repoList.sort(Comparator.comparingInt((Tuple2<String, Integer> tuple) -> tuple.f1).reversed());

            // Retrieve the top 10
            List<Tuple2<String, Integer>> top10 = new ArrayList<>(repoList.subList(0, Math.min(10, repoList.size())));

            // System.out.println("-------------"+ top10.toString());
            out.collect(top10);
        }
    }
}
