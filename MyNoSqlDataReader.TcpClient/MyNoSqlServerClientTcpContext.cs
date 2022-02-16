using System.Text;
using MyNoSqlDataReader.Core.SyncEvents;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlDataReader.TcpClient;

public class MyNoSqlServerClientTcpContext : ClientTcpContext<IMyNoSqlTcpContract>
{

    private readonly Action<SyncContract> _handleSyncContract;
    private readonly string _appName;

    private readonly IReadOnlyList<string> _tablesToSubscribe;

    public MyNoSqlServerClientTcpContext(Action<SyncContract> handleSyncContract,
        IReadOnlyList<string> tablesToSubscribe, string appName)
    {
        _handleSyncContract = handleSyncContract;
        _tablesToSubscribe = tablesToSubscribe;
        _appName = appName;
    }
    



    private static readonly Lazy<string> GetReaderVersion = new(() =>
    {
        try
        {
            return typeof(MyNoSqlServerClientTcpContext).Assembly.GetName().Version?.ToString() ?? string.Empty;
        }
        catch (Exception)
        {
            return "unknown";
        }
    });


    protected override ValueTask OnConnectAsync()
    {

        var readerVersion = GetReaderVersion.Value;

        readerVersion = ";ReaderVersion:" + readerVersion;

        var greetingsContract = new GreetingContract
        {
            Name = _appName + readerVersion
        };

        SendDataToSocket(greetingsContract);

        foreach (var tableToSubscribe in _tablesToSubscribe)
        {
            var subscribePacket = new SubscribeContract
            {
                TableName = tableToSubscribe
            };

            SendDataToSocket(subscribePacket);

            Console.WriteLine("Subscribed to MyNoSql table: " + tableToSubscribe);
        }

        return new ValueTask();

    }

    protected override ValueTask OnDisconnectAsync()
    {
        return new ValueTask();
    }

    protected override ValueTask HandleIncomingDataAsync(IMyNoSqlTcpContract data)
    {

        try
        {
            switch (data)
            {

                case InitTableContract initTableContract:

                    var initTableSyncContract = new SyncContract(initTableContract.TableName, SyncEventType.InitTable,
                        initTableContract.Data);
                    _handleSyncContract(initTableSyncContract);
                    break;

                case InitPartitionContract initPartitionContract:
                    var updatePartitionSyncContract = new SyncContract(initPartitionContract.TableName,
                        SyncEventType.InitPartitions,  MapToInitPartitionSyncContract(initPartitionContract));
                    _handleSyncContract(updatePartitionSyncContract);
                    break;

                case UpdateRowsContract updateRowsContract:
                    var updateRowsSyncContract = new SyncContract(updateRowsContract.TableName, SyncEventType.UpdateRows,
                        updateRowsContract.Data);
                    _handleSyncContract(updateRowsSyncContract);
                    break;

                case DeleteRowsContract deleteRowsContract:
                    var deleteRowsSyncContract = new SyncContract(deleteRowsContract.TableName,
                        SyncEventType.DeleteRows, MapToDeleteSyncContract(deleteRowsContract.RowsToDelete));
                    _handleSyncContract(deleteRowsSyncContract);
                    break;

            }
        }
        catch (Exception e)
        {
            Console.WriteLine("There is a problem with Packet: " + data.GetType());
            Console.WriteLine(e);
            throw;
        }

        return new ValueTask();
    }

    protected override IMyNoSqlTcpContract GetPingPacket()
    {
        return PingContract.Instance;
    }

    private static byte[] MapToDeleteSyncContract(IReadOnlyList<(string PartitionKey, string RowKey)> src)
    {
        var contract = new Dictionary<string, List<string>>();

        foreach (var (pk, rk) in src)
        {
            if (!contract.ContainsKey(pk))
                contract.Add(pk, new List<string>());
            contract[pk].Add(rk);
        }

        var result = Newtonsoft.Json.JsonConvert.SerializeObject(contract);

        return Encoding.UTF8.GetBytes(result);
    }
    
    private static byte[] MapToInitPartitionSyncContract(InitPartitionContract initPartitionContract)
    {

        var result = new List<byte>
        {
            (byte) '{',
            (byte) '"'
        };

        result.AddRange(Encoding.UTF8.GetBytes(initPartitionContract.PartitionKey));
        result.Add((byte)'"');
        result.Add((byte)':');
        result.AddRange(initPartitionContract.Data);
        result.Add((byte)'}');
        return result.ToArray();

    }
}