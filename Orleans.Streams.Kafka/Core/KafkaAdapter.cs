﻿using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Producer;
using Orleans.Streams.Kafka.Serialization;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapter : IQueueAdapter, IDisposable
	{
		private readonly KafkaStreamOptions _options;
		private readonly IDictionary<string, QueueProperties> _queueProperties;
		private readonly Serializer<KafkaBatchContainer> _serializer;
		private readonly ILoggerFactory _loggerFactory;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly IProducer<byte[], KafkaBatchContainer> _producer;
		private readonly ILogger<KafkaAdapter> _logger;

		public string Name { get; }
		public bool IsRewindable { get; } = false; // todo: provide way to pass sequence token (offset) so that we can rewind
		public StreamProviderDirection Direction { get; } = StreamProviderDirection.ReadWrite;

		public KafkaAdapter(
			string providerName,
			KafkaStreamOptions options,
			IDictionary<string, QueueProperties> queueProperties,
			Serializer<KafkaBatchContainer> serializer,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options;
			_queueProperties = queueProperties;
			_serializer = serializer;
			_loggerFactory = loggerFactory;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_logger = _loggerFactory.CreateLogger<KafkaAdapter>();

			Name = providerName;

			_producer = new ProducerBuilder<byte[], KafkaBatchContainer>(options.ToProducerProperties())
				.SetValueSerializer(new ConfluentKafkaBatchContainerSerializer(serializer))
				.Build();
		}

		public async Task QueueMessageBatchAsync<T>(
			StreamId streamId,
			IEnumerable<T> events,
			StreamSequenceToken token,
			Dictionary<string, object> requestContext
		)
		{
			try
			{
				var eventList = events.Cast<object>().ToList();
				if (eventList.Count == 0)
					return;

				var batch = new KafkaBatchContainer(
					streamId,
					eventList,
					_options.ImportRequestContext ? requestContext : null,
					token as EventSequenceTokenV2
				);

				await _producer.Produce(batch);
			}
			catch (Exception ex)
			{
				_logger.LogError(
					ex, "Failed to publish message: streamNamespace: {namespace}, streamKey: {key}",
					streamId.GetNamespace(),
					streamId.GetKeyAsString()
				);

				throw;
			}
		}

		public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
			=> new KafkaAdapterReceiver(
				Name,
				_queueProperties[queueId.GetStringNamePrefix()],
				_options,
				_serializer,
				_loggerFactory,
				_grainFactory,
				_externalDeserializer
			);

		public void Dispose()
		{
			_producer.Flush(TimeSpan.FromSeconds(2));
			_producer.Dispose();

			GC.SuppressFinalize(this);
		}
	}
}