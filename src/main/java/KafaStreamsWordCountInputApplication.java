import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class KafaStreamsWordCountInputApplication {

    private static final Logger logger = Logger.getLogger(KafaStreamsWordCountInputApplication.class.getName());

    private static final String TOPIC_INPUT = "word-count-input";
    private static final String TOPIC_OUTPUT = "word-count-output";

    public static void main(String[] args) throws Exception {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, TOPIC_INPUT);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(TOPIC_INPUT);
        KTable<String, Long> wordCounts =
                textLines.mapValues(line -> line.toLowerCase())
                .flatMapValues(line -> Arrays.asList(line.split("\\W+")))
                .selectKey((key,word) -> word)
                .groupByKey()
                .count("Counts");

        // Escreve os dados no kafka
        wordCounts.to(Serdes.String(), Serdes.Long(), TOPIC_OUTPUT);

        // Executa a aplicação
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // Fechando o processamento ao terminar a aplicação
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
