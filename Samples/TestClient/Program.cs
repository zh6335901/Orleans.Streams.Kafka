using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using TestGrains;

namespace TestClient
{
	internal static class Program
	{
		private static async Task Main(string[] args)
		{
			Console.Title = "Client";

			using IHost host = GetOrleansClientHost();
			await host.StartAsync();

			var clusterClient = host.Services.GetRequiredService<IClusterClient>();

			var grainId = "PLAYER-5a98c80e-26b8-4d1c-a5da-cb64237f2392";
			var testGrain = clusterClient.GetGrain<ITestGrain>(grainId);

			var result = await testGrain.GetThePhrase();

			Console.BackgroundColor = ConsoleColor.DarkMagenta;
			Console.WriteLine(result);

			var streamProvider = clusterClient.GetStreamProvider("KafkaProvider");
			var stream = streamProvider.GetStream<TestModel>("streamId", "sucrose-test");

			string line;
			while ((line = Console.ReadLine()) != string.Empty)
			{
				await stream.OnNextAsync(new TestModel
				{
					Greeting = line
				});
			}

			Console.ReadKey();
		}

		private static IHost GetOrleansClientHost()
		{
			var host = new HostBuilder()
				.ConfigureLogging(loggingBuilder => loggingBuilder.AddConsole())
				.UseOrleansClient(clientBuilder =>
				{
					var siloAddress = IPAddress.Loopback;
					var gatewayPort = 30000;

					var brokers = new List<string>
					{
						"[host name]:39000",
						"[host name]:39001",
						"[host name]:39002"
					};

					clientBuilder
						.Configure<ClusterOptions>(options =>
						{
							options.ClusterId = "TestCluster";
							options.ServiceId = "123";
						})
						.UseConnectionRetryFilter<ClientConnectRetryFilter>()
						.UseStaticClustering(options => options.Gateways.Add((new IPEndPoint(siloAddress, gatewayPort)).ToGatewayUri()))
						.AddKafka("KafkaProvider")
						.WithOptions(options =>
						{
							options.BrokerList = brokers;
							options.ConsumerGroupId = "TestGroup";
							options.AddTopic("sucrose-test");
						})
						.Build();
				})
				.Build();

			return host;
		}

		internal sealed class ClientConnectRetryFilter : IClientConnectionRetryFilter
		{
			private int _retryCount = 0;
			private const int _maxRetry = 25;
			private const int _delay = 3_000;

			public async Task<bool> ShouldRetryConnectionAttempt(
				Exception exception,
				CancellationToken cancellationToken)
			{
				if (_retryCount >= _maxRetry)
				{
					return false;
				}

				if (!cancellationToken.IsCancellationRequested &&
					exception is SiloUnavailableException siloUnavailableException)
				{
					_retryCount++;

					Console.WriteLine(
						$"Attempt {_retryCount} of {_maxRetry} failed to initialize the Orleans client.");

					await Task.Delay(_delay, cancellationToken);

					return true;
				}

				return false;
			}
		}
	}
}