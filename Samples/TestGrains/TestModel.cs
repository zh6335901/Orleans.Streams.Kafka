using Orleans;

namespace TestGrains
{
	[GenerateSerializer]
	public class TestModel
	{
		[Id(0)]
		public string Greeting { get; set; }

		public override string ToString() => Greeting;
	}
}