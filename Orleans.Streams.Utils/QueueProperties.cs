﻿using System;

namespace Orleans.Streams.Utils
{
	public class QueueProperties
	{
		public string QueueName { get; }
		public string Namespace { get; }
		public uint PartitionId { get; }
		public bool IsExternal { get; }
		public Type ExternalContractType { get; }

		public QueueProperties(
			string @namespace,
			uint partitionId = 0,
			bool isExternal = false,
			Type externalContractType = null
		)
		{
			Namespace = @namespace;
			PartitionId = partitionId;
			QueueName = $"{@namespace}_{partitionId}";
			IsExternal = isExternal;
			ExternalContractType = externalContractType;
		}
	}
}