

```C#

var url = "127.0.0.1:5125";


var myNoSqlDataReaderConnection = new MyNoSqlDataReaderTcpConnection(()=>url, "Test");

var quotes = myNoSqlDataReaderConnection.Subscribe<QuoteProfileMyNoSqlEntity>("quoteprofile");


quotes.RegisterRowsUpdatesCallback(updates =>
{
    if (updates.Updated != null)
    {
        Console.WriteLine($"Has updated:{updates.Updated.Count}");
    }
    
    if (updates.Deleted != null)
    {
        Console.WriteLine($"Has deleted:{updates.Deleted.Count}" );
    }
});

myNoSqlDataReaderConnection.Start();
```
