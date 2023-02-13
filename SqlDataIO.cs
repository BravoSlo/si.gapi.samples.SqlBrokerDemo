using System.Data.SqlClient;
using System.Data;

namespace si.gapi.samples.SqlBrokerDemo;
internal class SqlDataIO {

	#region // public //
	public void Run() {
		string sqlSetupQuery = File.ReadAllText("sql_setup.sql");
		using SqlConnection sqlConn = new SqlConnection(Program.CONN_STRING);
		sqlConn.Open();

		for(int i = 0; i < 10; i++) {
			insertTableSourceOne(sqlConn, $"Data Index {i}");
			insertTableSourceTwo(sqlConn, $"Data Index {100-i}");
		}

		sqlConn.Close();
	}
	#endregion

	#region // private //
	private void insertTableSourceOne(SqlConnection sqlConn, string data) {
        Console.WriteLine($"DATA: TableSourceOne insert {data}");
        using SqlCommand sqlCmd = sqlConn.CreateCommand();
		sqlCmd.CommandType = CommandType.Text;
		sqlCmd.CommandText = "insert into [BrokerSource01].[dbo].[TableSourceOne] ([data]) values (@data)";
		sqlCmd.Parameters.Add(new SqlParameter("@data", data));
		sqlCmd.ExecuteNonQuery();
	}
	private void insertTableSourceTwo(SqlConnection sqlConn, string data) {
		Console.WriteLine($"DATA: TableSourceTwo insert {data}");
		using SqlCommand sqlCmd = sqlConn.CreateCommand();
		sqlCmd.CommandType = CommandType.Text;
		sqlCmd.CommandText = "insert into [BrokerSource02].[dbo].[TableSourceTwo] ([data]) values (@data)";
		sqlCmd.Parameters.Add(new SqlParameter("@data", data));
		sqlCmd.ExecuteNonQuery();
	}
	#endregion

}
