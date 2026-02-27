using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using Orleans.Serialization;
using Orleans.Streams.Kafka.Config;
using Orleans.Streams.Kafka.Consumer;
using Orleans.Streams.Utils;
using Orleans.Streams.Utils.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;

namespace Orleans.Streams.Kafka.Core
{
	public class KafkaAdapterReceiver : IQueueAdapterReceiver
	{
		private readonly ILogger<KafkaAdapterReceiver> _logger;
		private readonly string _providerName;
		private readonly KafkaStreamOptions _options;
		private readonly Serializer<KafkaBatchContainer> _serializer;
		private readonly IGrainFactory _grainFactory;
		private readonly IExternalStreamDeserializer _externalDeserializer;
		private readonly QueueProperties _queueProperties;
		private readonly TopicPartition _topicPartition;
		private readonly SemaphoreSlim _consumerLock = new SemaphoreSlim(1, 1);
		private readonly SortedSet<long> _deliveredOffsets = new SortedSet<long>();

		private IConsumer<byte[], byte[]> _consumer;
		private Task _commitPromise = Task.CompletedTask;
		private Task<IList<IBatchContainer>> _consumePromise;
		private long? _nextCommitOffset;
		private bool _shutdownRequested;

		public KafkaAdapterReceiver(
			string providerName,
			QueueProperties queueProperties,
			KafkaStreamOptions options,
			Serializer<KafkaBatchContainer> serializer,
			ILoggerFactory loggerFactory,
			IGrainFactory grainFactory,
			IExternalStreamDeserializer externalDeserializer
		)
		{
			_options = options ?? throw new ArgumentNullException(nameof(options));

			_providerName = providerName;
			_queueProperties = queueProperties;
			_serializer = serializer;
			_grainFactory = grainFactory;
			_externalDeserializer = externalDeserializer;
			_topicPartition = new TopicPartition(_queueProperties.Namespace, (int)_queueProperties.PartitionId);
			_logger = loggerFactory.CreateLogger<KafkaAdapterReceiver>();
		}

		public Task Initialize(TimeSpan timeout)
		{
			_consumer = new ConsumerBuilder<byte[], byte[]>(_options.ToConsumerProperties())
				.SetErrorHandler((sender, errorEvent) =>
					_logger.LogError(
						"Consume error reason: {reason}, code: {code}, is broker error: {errorType}",
						errorEvent.Reason,
						errorEvent.Code,
						errorEvent.IsBrokerError
					))
				.Build();

			var offsetMode = Offset.Stored;
			switch (_options.ConsumeMode)
			{
				case ConsumeMode.LastCommittedMessage:
					offsetMode = Offset.Stored;
					break;
				case ConsumeMode.StreamEnd:
					offsetMode = Offset.End;
					break;
				case ConsumeMode.StreamStart:
					offsetMode = Offset.Beginning;
					break;
			}

			_consumer.Assign(new TopicPartitionOffset(_topicPartition, offsetMode));

			return Task.CompletedTask;
		}

		public Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
		{
			var consumerRef = _consumer; // store direct ref, in case we are somehow asked to shutdown while we are receiving.

			if (consumerRef == null)
				return Task.FromResult<IList<IBatchContainer>>(new List<IBatchContainer>());

			var cancellationSource = new CancellationTokenSource();
			cancellationSource.CancelAfter(_options.PollBufferTimeout);

			_consumePromise = Task.Run(
				() => PollForMessages(
					maxCount,
					cancellationSource
				),
				cancellationSource.Token
			);

			return _consumePromise;
		}

		public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
		{
			if (_shutdownRequested || messages == null || !messages.Any())
				return;

			_commitPromise = CommitDeliveredOffsetsAsync(messages);
			await _commitPromise;
		}

		public async Task Shutdown(TimeSpan timeout)
		{
			_shutdownRequested = true;

			try
			{
				var tasks = new List<Task>();

				if (_commitPromise != null)
					tasks.Add(_commitPromise);

				if (_consumePromise != null)
					tasks.Add(_consumePromise);

				await Task.WhenAll(tasks);
			}
			finally
			{
				await _consumerLock.WaitAsync();

				try
				{
					var consumerRef = _consumer;
					if (consumerRef != null)
					{
						consumerRef.Unassign();
						consumerRef.Unsubscribe();
						consumerRef.Close();
						consumerRef.Dispose();
						_consumer = null;
						_deliveredOffsets.Clear();
					}
				}
				finally
				{
					_consumerLock.Release();
				}
			}
		}

		private async Task<IList<IBatchContainer>> PollForMessages(int maxCount, CancellationTokenSource cancellation)
		{
			try
			{
				var batches = new List<IBatchContainer>();
				for (var i = 0; i < maxCount && !cancellation.IsCancellationRequested; i++)
				{
					ConsumeResult<byte[], byte[]> consumeResult = null;

					await _consumerLock.WaitAsync(cancellation.Token);
					try
					{
						if (_shutdownRequested || _consumer == null)
							break;

						consumeResult = _consumer.Consume(_options.PollTimeout);
						if (consumeResult != null && !_nextCommitOffset.HasValue)
							_nextCommitOffset = consumeResult.Offset.Value;
					}
					finally
					{
						_consumerLock.Release();
					}

					if (consumeResult == null)
						break;

					var batchContainer = consumeResult.ToBatchContainer(
						new SerializationContext
						{
							Serializer = _serializer,
							ExternalStreamDeserializer = _externalDeserializer
						},
						_queueProperties
					);

					await TrackMessage(batchContainer);

					batches.Add(batchContainer);
				}

				return batches;
			}
			catch (OperationCanceledException ex) when (ex.CancellationToken.IsCancellationRequested)
			{
				return new List<IBatchContainer>();
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to poll for messages queueId: {@queueProperties}", _queueProperties);
				throw;
			}
			finally
			{
				cancellation.Dispose();
			}
		}

		private Task TrackMessage(IBatchContainer container)
		{
			if (!_options.MessageTrackingEnabled)
				return Task.CompletedTask;

			var trackingGrain = _grainFactory.GetMessageTrackerGrain(_providerName, _queueProperties.QueueName);
			return trackingGrain.Track(new Immutable<IBatchContainer>(container));
		}

		private async Task CommitDeliveredOffsetsAsync(IList<IBatchContainer> messages)
		{
			var deliveredOffsets = messages
				.OfType<KafkaBatchContainer>()
				.Select(batch => batch.TopicPartitionOffSet.Offset.Value)
				.OrderBy(offset => offset)
				.ToList();

			if (deliveredOffsets.Count == 0)
				return;

			var lockTaken = false;
			try
			{
				await _consumerLock.WaitAsync();
				lockTaken = true;

				if (_shutdownRequested || _consumer == null)
					return;

				_nextCommitOffset ??= deliveredOffsets[0];
				foreach (var offset in deliveredOffsets)
				{
					if (offset >= _nextCommitOffset.Value)
						_deliveredOffsets.Add(offset);
				}

				if (_deliveredOffsets.Count == 0)
					return;

				var initialNextOffset = _nextCommitOffset.Value;
				var nextOffsetToCommit = initialNextOffset;

				while (_deliveredOffsets.Remove(nextOffsetToCommit))
					nextOffsetToCommit++;

				if (nextOffsetToCommit > initialNextOffset)
				{
					_consumer.Commit(new[] { new TopicPartitionOffset(_topicPartition, new Offset(nextOffsetToCommit)) });
					_nextCommitOffset = nextOffsetToCommit;
					return;
				}

				var firstPendingDeliveredOffset = _deliveredOffsets.Min;
				if (firstPendingDeliveredOffset > initialNextOffset)
				{
					_logger.LogWarning(
						"Detected non-contiguous delivered offsets. Seeking back to retry from offset {offset}. queueId: {@queueProperties}",
						initialNextOffset,
						_queueProperties
					);

					_consumer.Seek(new TopicPartitionOffset(_topicPartition, new Offset(initialNextOffset)));
					_deliveredOffsets.Clear();
				}
			}
			catch (Exception ex)
			{
				var lastDeliveredOffset = deliveredOffsets.Count == 0 ? (long?)null : deliveredOffsets[^1];
				_logger.LogError(ex, "Failed to commit message offset: {offset}", lastDeliveredOffset);
				throw;
			}
			finally
			{
				if (lockTaken)
					_consumerLock.Release();
			}
		}
	}
}
