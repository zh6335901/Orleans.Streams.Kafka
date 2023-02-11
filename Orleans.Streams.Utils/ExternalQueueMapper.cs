using Orleans.Runtime;
using Orleans.Streams.Utils.Tools;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace Orleans.Streams.Utils
{
	public class ExternalQueueMapper : IConsistentRingStreamQueueMapper
	{
		private readonly IReadOnlyDictionary<string, HashRing> _queueMap;

		public ExternalQueueMapper(IEnumerable<QueueProperties> queues)
		{
			_queueMap = new ReadOnlyDictionary<string, HashRing>(CreateQueueMap(queues));
		}

		public IEnumerable<QueueId> GetAllQueues()
			=> _queueMap.Values
				.SelectMany(queueMap => queueMap.GetAllRingMembers());

		public QueueId GetQueueForStream(StreamId streamId)
		{
			var streamNamespace = streamId.GetNamespace();

			if (!_queueMap.ContainsKey(streamNamespace))
				throw new ArgumentException("No queue for supplied namespace");

			return _queueMap[streamNamespace].CalculateResponsible((uint)streamId.GetHashCode());
		}

		public IEnumerable<QueueId> GetQueuesForRange(IRingRange range)
			=> from ring in _queueMap.Values
			   from queueId in ring.GetAllRingMembers()
			   where range.InRange(queueId.GetUniformHashCode())
			   select queueId;

		private static IDictionary<string, HashRing> CreateQueueMap(
			IEnumerable<QueueProperties> queueProps
		) => queueProps
				.GroupBy(props => props.Namespace)
				.Select((grouping, index)=> (Grouping: grouping, Index: index))
				.ToDictionary(
					g => g.Grouping.Key,
					g =>
					{
						var ringSize = g.Grouping.Count();
						var queueIds = g.Grouping
							.Select((props, iteration) =>
							{
								var uniformHashCode = (uint)0;
								var queueIndex = (uint)g.Index;

								if (ringSize == 1)
									return QueueId.GetQueueId(props.QueueName, queueIndex, uniformHashCode);

								var portion = checked((uint)(RangeFactory.RING_SIZE / ringSize + 1));
								uniformHashCode = checked(portion * (uint)iteration);
								return QueueId.GetQueueId(props.QueueName, queueIndex, uniformHashCode);
							})
							.ToArray();

						return new HashRing(queueIds);
					});
	}
}