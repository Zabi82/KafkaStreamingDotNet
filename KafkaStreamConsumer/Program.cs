using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaStreamConsumer {
    class Program {

        const string BOOTSTRAP_SERVERS = "localhost:9092";
        const int NUM_MESSAGES = 10;
        const int TOTAL_PARTITIONS = 1;
        const int TOTAL_REPLICATIONS = 1;

        const String CONSUMER_GROUP_ID = "testgroup_1";

        static async Task CreateTopic(string name, ClientConfig config) {
            using (var adminClient = new AdminClientBuilder(config).Build()) {
                try {
                    await adminClient.CreateTopicsAsync(new List<TopicSpecification> {
                        new TopicSpecification { Name = name, NumPartitions = TOTAL_PARTITIONS, ReplicationFactor = TOTAL_REPLICATIONS } });
                } catch (CreateTopicsException e) {
                    if (e.Results[0].Error.Code != ErrorCode.TopicAlreadyExists) {
                        Console.WriteLine($"An error occured creating topic {name}: {e.Results[0].Error.Reason}");
                    } else {
                        Console.WriteLine("Topic already exists");
                    }
                }
            }
        }

        static void Consume(string topic, ClientConfig config) {
            var consumerConfig = new ConsumerConfig(config);
            consumerConfig.GroupId = CONSUMER_GROUP_ID;
            consumerConfig.AutoOffsetReset = AutoOffsetReset.Earliest;
            consumerConfig.EnableAutoCommit = true;

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            using (var consumer = new ConsumerBuilder<string, long>(consumerConfig).Build()) {
                consumer.Subscribe(topic);
                try {
                    while (true) {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed record with key {cr.Message.Key} and value {cr.Message.Value}");
                    }
                } catch (OperationCanceledException) {
                    // Ctrl-C was pressed.
                } finally {
                    consumer.Close();
                }
            }
        }

        static async Task Main(string[] args) {
            var config = new ClientConfig(new Dictionary<string, string>() { {"bootstrap.servers", BOOTSTRAP_SERVERS} });
            Consume("word_count_output_topic", config);
        }
    }
}