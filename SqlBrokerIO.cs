using System.Data;
using System.Data.SqlClient;

namespace si.gapi.samples.SqlBrokerDemo;
internal class SqlBrokerIO {

    #region // locals //
    private const string receiveProcedure = "[dbo].[BrokerMainReceiveMessage]";
	private bool _running = false;
	private Task? _receiver;
    #endregion

    #region // events //
    public event EventHandler<string>? OnMessage;
    #endregion

    #region // ctor //
    public SqlBrokerIO() {
    }
	#endregion

	#region // public //
	public void Start() {
		_running = true;
		_receiver = new Task(() => {
			while(_running) {
				receive();
			}
		});
		_receiver.Start();
	}
	public void Stop() {
		_running = false;
		_receiver?.Wait();
		_receiver = null;
	}
	#endregion

	#region // private //
	private void receive() {
		using SqlConnection sqlConn = new SqlConnection(Program.CONN_STRING);
		using SqlCommand sqlCmd = sqlConn.CreateCommand();
		sqlCmd.CommandText = receiveProcedure;
		sqlCmd.CommandType = CommandType.StoredProcedure;
		sqlCmd.Parameters.Add("messageType", SqlDbType.VarChar, 500);
		sqlCmd.Parameters.Add("messageData", SqlDbType.VarChar, 1000);
		sqlCmd.Parameters["messageType"].Direction = ParameterDirection.Output;
		sqlCmd.Parameters["messageData"].Direction = ParameterDirection.Output;
		sqlConn.Open();
		sqlCmd.ExecuteNonQuery();
		string? messageType = sqlCmd.Parameters["messageType"].Value.ToString();
		string? messageData = sqlCmd.Parameters["messageData"].Value.ToString();
		if (!string.IsNullOrEmpty(messageType) && !string.IsNullOrEmpty(messageData))
			OnMessage?.Invoke(this, messageData);
		sqlConn.Close();
	}
	#endregion

}
