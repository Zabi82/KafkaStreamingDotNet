using System;

namespace KafkaStreamClient {
    class Program {

        const string BOOTSTRAP_SERVERS = "localhost:9092";

        const string APPLICATION_ID = "wordcount";

        const string INPUT_TEXT_TOPIC = "input_text_topic";

        const string WORD_COUNT_OUTPUT_TOPIC = "word_count_output_topic";

        static void Main(string[] args) {
            var config = new StreamConfig<StringSerDes, StringSerDes>();
            config.ApplicationId = APPLICATION_ID;
            config.BootstrapServers = BOOTSTRAP_SERVERS;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            
            StreamBuilder builder = new StreamBuilder();

            var kstream = builder.Stream<string, string>(INPUT_TEXT_TOPIC)
                                .Map((k, v) => KeyValuePair.Create(v, v))
                                .FilterNot((k, v) => v.contains("the"))
                                .GroupBy((k, v) => v)
                                .Count()
                                .To(WORD_COUNT_OUTPUT_TOPIC);
            builder.Table("table", InMemory<string, string>.As("table-store"));

            Topology t = builder.Build();
            KafkaStream stream = new KafkaStream(t, config);

            Console.CancelKeyPress += (o, e) => {
                stream.Dispose();
            };

            await stream.StartAsync();

        }
    }
}
