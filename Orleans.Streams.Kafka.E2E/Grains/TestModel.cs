using System;

namespace Orleans.Streams.Kafka.E2E.Grains
{
	public class TestModel
	{
		public int NumberOfLegs { get; set; }

		public int NumberOfHeads { get; set; }

		public bool IsLastMessage { get; set; }

		public override bool Equals(object obj)
		{
			var mod = (TestModel)obj;
			return mod.NumberOfHeads == NumberOfHeads && mod.NumberOfLegs == NumberOfLegs;
		}

		public static TestModel Random()
			=> new TestModel
			{
				NumberOfHeads = System.Random.Shared.Next(1000),
				NumberOfLegs = System.Random.Shared.Next(1000)
			};
	}

	public class TestResult
	{
		public TestModel Expected { get; set; }
		public TestModel Actual { get; set; }
	}
}