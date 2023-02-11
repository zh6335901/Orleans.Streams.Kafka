using Confluent.Kafka;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils;
using System.Collections.Generic;
using System.Text;
using SerializationContext = Orleans.Streams.Kafka.Serialization.SerializationContext;

namespace Orleans.Streams.Kafka.Consumer
{
	public static class ConsumeResultExtensions
	{
		public static KafkaBatchContainer ToBatchContainer(
			this ConsumeResult<byte[], byte[]> result,
			SerializationContext serializationContext,
			QueueProperties queueProperties
		)
		{
			var sequence = new EventSequenceTokenV2(result.Offset.Value);

			if (queueProperties.IsExternal)
			{
				var streamId = StreamId.Create(Encoding.UTF8.GetBytes(queueProperties.Namespace), result.Message.Key);

				var message = serializationContext
					.ExternalStreamDeserializer
					.Deserialize(queueProperties, queueProperties.ExternalContractType, result.Message.Value);

				return new KafkaBatchContainer(
					streamId,
					new List<object> { message },
					null,
					sequence,
					result.TopicPartitionOffset
				);
			}

			var serializer = serializationContext.Serializer;
			var batchContainer = serializer.Deserialize(result.Message.Value);

			batchContainer.RealSequenceToken ??= sequence;
			batchContainer.TopicPartitionOffSet = result.TopicPartitionOffset;

			return batchContainer;
		}
	}
}
