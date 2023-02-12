using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streams.Kafka.Config;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;

namespace TestSilo
{
	internal static class Program
	{
		public static async Task Main(string[] args)
		{
			Console.Title = "Silo 1";

			const int siloPort = 11111;
			const int gatewayPort = 30000;
			var siloAddress = IPAddress.Loopback;

			var brokers = new List<string>
			{
				"127.0.0.1:9093"
			};

			using IHost host = Host.CreateDefaultBuilder(args)
				.UseOrleans(builder =>
				{
					builder.Configure<ClusterOptions>(options =>
					{
						//options.SiloName = "TestCluster";
						options.ClusterId = "TestCluster";
						options.ServiceId = "123";
					})
					.UseDevelopmentClustering(options => options.PrimarySiloEndpoint = new IPEndPoint(siloAddress, siloPort))
					.ConfigureEndpoints(siloAddress, siloPort, gatewayPort)
					.ConfigureLogging(logging => logging.AddConsole())
					.AddMemoryGrainStorageAsDefault()
					.AddMemoryGrainStorage("PubSubStore")
					.AddKafka("KafkaProvider")
					.WithOptions(options =>
					{
						options.BrokerList = brokers.ToArray();
						options.ConsumerGroupId = "TestGroup";
						options.MessageTrackingEnabled = true;
						options.AddTopic("sucrose-test", new TopicCreationConfig { AutoCreate = true, Partitions = 2 });
						options.AddTopic("sucrose-auto", new TopicCreationConfig { AutoCreate = true, Partitions = 2, ReplicationFactor = 1, RetentionPeriodInMs = 86400000 });
						options.AddTopic("sucrose-auto2", new TopicCreationConfig { AutoCreate = true, Partitions = 3, ReplicationFactor = 1, RetentionPeriodInMs = 86400000 });
					})
					.AddLoggingTracker()
					.Build();
				})
				.Build();

			await host.StartAsync();

			Console.ReadKey();

			await host.StopAsync();
		}
	}
}