using MyNoSqlDataReader.Core.SyncEvents;
using MyNoSqlDataReader.Core;
using MyNoSqlServer.Abstractions;
using MyNoSqlServer.TcpContracts;
using MyTcpSockets;

namespace MyNoSqlDataReader.TcpClient;

public class MyNoSqlDataReaderTcpConnection
{
    public enum LogLevel{
        Error, Info
    }

    public struct LogItem
    {
        public DateTime DateTime { get; private set; }
        public LogLevel Level { get; private set; }
        public string Process { get; private set; }
        public string Message { get; private set; }
        public Exception? Exception { get; private set; }
        
        public static LogItem Create(LogLevel logLevel, string process, string message, Exception? exception)
        {
            return new LogItem
            {
                DateTime = DateTime.UtcNow,
                Level = logLevel,
                Process = process,
                Message = message,
                Exception = exception
            };
        }
    }

    
    private readonly Dictionary<string, IMyNoSqlDataReaderEventUpdater> _subscribers = new();
    
    private readonly MyClientTcpSocket<IMyNoSqlTcpContract> _tcpClient;
    
    private static readonly Lazy<string> ReaderVersion = new (() =>
    {
        try
        {
            return typeof(MyNoSqlDataReaderTcpConnection).Assembly.GetName().Version?.ToString() ?? "Unknown";
        }
        catch (Exception)
        {
            return "Unknown";
        }
    });
    
    public MyNoSqlDataReaderTcpConnection(Func<string> getHostPort, string appName)
    {
        _tcpClient = new MyClientTcpSocket<IMyNoSqlTcpContract>(getHostPort, TimeSpan.FromSeconds(3));

        _tcpClient
            .RegisterTcpContextFactory(() => new MyNoSqlServerClientTcpContext(HandleIncomingPacket, _subscribers.Keys.ToList(), appName))
            .Logs.AddLogInfo(WriteInfoLog)
            .Logs.AddLogException(WriteErrorLog)
            .RegisterTcpSerializerFactory(() => new MyNoSqlTcpSerializer());
    }

    private void WriteInfoLog(ITcpContext? _, string message)
    {
        if (_logCallback == null)
        {
            Console.WriteLine("MyNoSqlDataReader Info: " + message);
        }
        else
        {
            _logCallback(LogItem.Create(LogLevel.Info, "MyNoSqlTcpDataReader", message, null));
        }
        
    }
    
    private void WriteErrorLog(ITcpContext? _, Exception e)
    {
        if (_logCallback == null)
        {
            Console.WriteLine("MyNoSqlDataReader Error: " + e.Message);
            Console.WriteLine(e);
        }
        else
        {
            _logCallback(LogItem.Create(LogLevel.Error, "MyNoSqlTcpDataReader", e.Message, e));
        }
        
    }
        
    private Action<LogItem>? _logCallback;

    private void PlugLog(Action<LogItem> logCallback)
    {
        _logCallback = logCallback;
    }

    private void HandleIncomingPacket(SyncContract syncContract)
    {
        if (_subscribers.TryGetValue(syncContract.TableName, out var subscriber))
        {
            subscriber.UpdateData(syncContract);
        }
    } 
    
    public MyNoSqlDataReader<TDbRow> Subscribe<TDbRow>(string tableName) where TDbRow: IMyNoSqlEntity, new()
    {
        var parser = new TcpEventsParser<TDbRow>();
        var result = new MyNoSqlDataReader<TDbRow>(tableName, parser);
        _subscribers.Add(tableName, result);
        return result;
    }


    public async Task WaitAllTablesAreInitialized()
    {
        foreach (var (id, subscriber) in _subscribers)
        {
            WriteInfoLog(null, $"Waiting for table [{id}] is initialized....");
            Console.WriteLine($"Waiting for table [{id}] is initialized....");
            await subscriber.IsInitialized();
            WriteInfoLog(null, $"Table [{id}] is initialized!!!!!");
            Console.WriteLine($"Table [{id}] is initialized!!!!!");
            
        }
    }

    public void Start()
    {
        _tcpClient.Start();
    }
}