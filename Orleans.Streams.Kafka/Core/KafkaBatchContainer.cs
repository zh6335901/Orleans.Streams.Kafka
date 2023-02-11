using Confluent.Kafka;
using Newtonsoft.Json;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Orleans.Streams.Kafka.Core
{
	[Serializable]
	[GenerateSerializer]
	public class KafkaBatchContainer : IBatchContainer, IComparable<KafkaBatchContainer>
	{
		[JsonProperty("sequenceToken")]
		[Id(0)]
		private EventSequenceTokenV2 _sequenceToken;

		[JsonProperty("requestContext")]
		[Id(1)]
		private readonly Dictionary<string, object> _requestContext;

		[NonSerialized] internal TopicPartitionOffset TopicPartitionOffSet;

		public StreamSequenceToken SequenceToken => _sequenceToken;

		internal EventSequenceTokenV2 RealSequenceToken
		{ 
			get => _sequenceToken;
			set => _sequenceToken = value;
		}

		[JsonProperty]
		[Id(2)]
		protected List<object> Events { get; set; }

		[Id(3)]
		public StreamId StreamId { get; }

		[JsonConstructor]
		public KafkaBatchContainer(
			StreamId streamId,
			List<object> events,
			Dictionary<string, object> requestContext,
			EventSequenceTokenV2 sequenceToken
		) : this(streamId, events, requestContext, sequenceToken, null)
		{
		}

		public KafkaBatchContainer(
			StreamId streamId,
			List<object> events,
			Dictionary<string, object> requestContext,
			EventSequenceTokenV2 sequenceToken,
			TopicPartitionOffset offset
		)
		{
			Events = events ?? throw new ArgumentNullException(nameof(events), "Message contains no events.");

			StreamId = streamId;
			_sequenceToken = sequenceToken;
			TopicPartitionOffSet = offset;
			_requestContext = requestContext;
		}

		public virtual IEnumerable<Tuple<T, StreamSequenceToken>> GetEvents<T>()
		{
			var sequenceToken = (EventSequenceTokenV2)SequenceToken;
			return Events
				.OfType<T>()
				.Select((@event, iteration) =>
					Tuple.Create<T, StreamSequenceToken>(
						@event,
						sequenceToken.CreateSequenceTokenForEvent(iteration))
				);
		}

		public bool ImportRequestContext()
		{
			if (_requestContext == null)
				return false;

			foreach (var contextProperties in _requestContext)
				RequestContext.Set(contextProperties.Key, contextProperties.Value);

			return true;
		}

		public int CompareTo(KafkaBatchContainer other)
			=> TopicPartitionOffSet.Offset.Value.CompareTo(other.TopicPartitionOffSet.Offset.Value);

		public override string ToString()
			=> $"[{GetType().Name}:Stream={StreamId},#Items={Events.Count}]";
	}
}