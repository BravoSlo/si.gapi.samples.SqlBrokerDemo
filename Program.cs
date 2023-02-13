using System.Reflection;

namespace si.gapi.samples.SqlBrokerDemo;
internal class Program {

	#region // connection string //
	public const string CONN_STRING = "server=localhost;database=BrokerMain;integrated security=SSPI;MultipleActiveResultSets = true;";
	#endregion

	#region // entry point //
	static void Main(string[] args) {
        Console.Write("running setup ... ");
		SqlSetup sqlSetup = new SqlSetup();
		sqlSetup.Setup();
        Console.WriteLine("done");

        Console.Write("starting broker io ... ");
        SqlBrokerIO sbIO = new SqlBrokerIO();
		sbIO.OnMessage += (object? sender, string message) => Console.WriteLine($"BROKER: {message}");
		sbIO.Start();
        Console.WriteLine("done");

		Console.WriteLine("starting data io ... ");
		SqlDataIO sqlDataIO = new SqlDataIO();
		sqlDataIO.Run();
		Console.WriteLine("done");
        Console.WriteLine("finished ... broker still running");
        Console.ReadLine();
	}
	#endregion

}