using System.Data;
using System.Data.SqlClient;

namespace si.gapi.samples.SqlBrokerDemo;
internal class SqlSetup {

	#region // public //
	public void Setup() {
		string sqlSetupQuery = File.ReadAllText("sql_setup.sql");
		string[] sqlCommands = sqlSetupQuery.Split("\r\ngo\r\n", StringSplitOptions.RemoveEmptyEntries);

		using SqlConnection sqlConn = new SqlConnection(Program.CONN_STRING);
		sqlConn.Open();
		foreach (string command in sqlCommands) {
			using SqlCommand sqlCmd = sqlConn.CreateCommand();
			sqlCmd.CommandType = CommandType.Text;
			sqlCmd.CommandText = command;
			sqlCmd.ExecuteNonQuery();
		}
		sqlConn.Close();
	}
	#endregion

}
