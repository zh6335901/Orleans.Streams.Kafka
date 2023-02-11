using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;

namespace Orleans.Streams.Kafka.Serialization
{
	internal class ConfluentKafkaBatchContainerSerializer : Confluent.Kafka.ISerializer<KafkaBatchContainer>
	{
		private readonly Serializer<KafkaBatchContainer> _serializer;

		public ConfluentKafkaBatchContainerSerializer(Serializer<KafkaBatchContainer> serializer)
		{
			_serializer = serializer;
		}

		public byte[] Serialize(KafkaBatchContainer data, Confluent.Kafka.SerializationContext context)
			=> _serializer.SerializeToArray(data);
	}
}