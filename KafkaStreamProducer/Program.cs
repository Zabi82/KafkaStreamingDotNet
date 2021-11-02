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

namespace KafkaStreamProducer
 {
    class Program {

        const string BOOTSTRAP_SERVERS = "localhost:9092";
        const int NUM_MESSAGES = 10;
        const int TOTAL_PARTITIONS = 1;
        const int TOTAL_REPLICATIONS = 1;

        const string INPUT_TEXT_TOPIC = "input_text_topic";

        static string[] sentences = {"Kafka is a distributed middleware infrastructure", 
                                                " Kafka has a producer and consumer API",
												"Confluent Kafka provides enterprise support and provides additional components",
												"Stream API internally uses Kafka client API for producer and consumer",
												"Kafka streaming support at least once and exactly once processing"
                                                };

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
        
        static void Produce(string topic, ClientConfig config) {
            Random rnd = new Random();
            using (var producer = new ProducerBuilder<string, string>(config).Build()) {
                int numProduced = 0;
                for (int i=0; ; ++i) {
                    var key = i.ToString();
                    var val =  sentences[rnd.Next(0, 4)]; 

                    Console.WriteLine($"Producing record: {key} {val}");

                    producer.Produce(topic, new Message<string, string> { Key = key, Value = val },
                        (deliveryReport) => {
                            if (deliveryReport.Error.Code != ErrorCode.NoError) {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            } else {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                                numProduced += 1;
                            }
                        });
                    producer.Flush(TimeSpan.FromSeconds(10));
                    System.Threading.Thread.Sleep(2000);
                }
            }
        }

        static async Task Main(string[] args) {
            var config = new ClientConfig(new Dictionary<string, string>() { {"bootstrap.servers", BOOTSTRAP_SERVERS} });
            await CreateTopic(INPUT_TEXT_TOPIC, config);
            Produce(INPUT_TEXT_TOPIC, config);
        }
    }
}