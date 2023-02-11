using Orleans.Serialization;
using Orleans.Streams.Kafka.Core;
using Orleans.Streams.Utils.Serialization;

namespace Orleans.Streams.Kafka.Serialization
{
	public struct SerializationContext
	{
		public Serializer<KafkaBatchContainer> Serializer { get; set; }

		public IExternalStreamDeserializer ExternalStreamDeserializer { get; set; }
	}
}
