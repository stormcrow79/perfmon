using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace sql2influx
{
  class Program
  {
    static string SQL_LHMI_HANDLES = @"
select
  [@timestamp] = cast(cast(c1.CounterDateTime as varchar(23)) as datetime2(3)),
  [handles] = c1.CounterValue,
  [mem.working_set] = c2.CounterValue,
  [mem.virtual_mem] = c3.CounterValue,
  [mem.all_managed] = isnull(c4.CounterValue,0)
from CounterData c1 
  left join CounterData c2 on c1.GUID = c2.GUID and c1.RecordIndex = c2.RecordIndex and c2.CounterID = 14 -- pvt working set
  left join CounterData c3 on c1.GUID = c3.GUID and c1.RecordIndex = c3.RecordIndex and c3.CounterID = 13 -- virtual bytes
  left join CounterData c4 on c1.GUID = c4.GUID and c1.RecordIndex = c4.RecordIndex and c4.CounterID = 16 -- all managed heaps
where c1.CounterID = 10 -- handle count
order by c1.CounterDateTime";

    static string SQL_MNHHS_WCF_PERF = @"
select
  [@timestamp] = dateadd(hour, 2, cast(cast(c1.CounterDateTime as varchar(23)) as datetime2(3))),
  [wcf.pending_calls] = c1.CounterValue,
  [wcf.calls_per_sec] = c2.CounterValue,
  [wcf.pct_max_calls] = c3.CounterValue,
  [wcf.pct_max_sessions] = c4.CounterValue
from CounterData c1 
  left join CounterData c2 on c1.GUID = c2.GUID and c1.RecordIndex = c2.RecordIndex and c2.CounterID = 30 -- calls per second
  left join CounterData c3 on c1.GUID = c3.GUID and c1.RecordIndex = c3.RecordIndex and c3.CounterID = 42 -- percent of max calls
  left join CounterData c4 on c1.GUID = c4.GUID and c1.RecordIndex = c4.RecordIndex and c4.CounterID = 50 -- percent of max sessions
where c1.CounterID = 26 -- calls outstanding
order by c1.CounterDateTime";

    static string SQL_MNHHS_KARISMA_OPS = @"
select 
  [@timestamp] = dateadd(minute, datediff(minute, 0, timestamp), 0),
  [karisma.user_count] = cast(count(distinct [user_name]) as float), 
  [karisma.call_count] = cast(count(*) as float)
from operations
group by dateadd(minute, datediff(minute, 0, timestamp), 0)
order by dateadd(minute, datediff(minute, 0, timestamp), 0)";

    static void Main(string[] args)
    {
      var customer = "mnhhs";
      // need to lie to influx, since the perfmon times are "local"
      var timezone = TimeSpan.FromHours(10);

      var influx_home = @"C:\GitHub\tick\influxdb-1.5.2-1";

      var sql_server = "gavinm\\std17";
      var sql_dbname = $"perf_{customer}";

      var influx_dbname = $"perf_{customer}";
      var influx_measure = "karisma";
      //var import_filename = $@"c:\performance\{customer}-perf\{influx_measure}.txt";

      var queries = new string[] { SQL_MNHHS_WCF_PERF, SQL_MNHHS_KARISMA_OPS };

      foreach (var query in queries)
      {
        var import_filename = Path.GetTempFileName();
        var conn = new SqlConnection($"Data Source={sql_server};Initial Catalog={sql_dbname};Integrated Security=SSPI");
        conn.Open();

        var cmd = conn.CreateCommand();
        cmd.CommandText = query;

        var file = new StreamWriter(import_filename);
        file.NewLine = "\n";

        #region influx import file header
        //file.WriteLine("# DDL");
        //file.WriteLine($"CREATE DATABASE perf_{customer}");
        //file.WriteLine("CREATE RETENTION POLICY oneday ON pirates DURATION 1d REPLICATION 1");
        #endregion

        file.WriteLine("# DML");
        file.WriteLine($"# CONTEXT-DATABASE: {influx_dbname}");

        var count = 0;

        using (var reader = cmd.ExecuteReader())
        {
          while (reader.Read())
          {
            var line = new InfluxLine()
            {
              measurement = influx_measure,
              timestamp = new DateTimeOffset(reader.GetDateTime(0), timezone),
              fields = new Dictionary<string, object>()
            };

            for (var i = 1; i < reader.FieldCount; i++)
              line.fields.Add(reader.GetName(i), reader.GetDouble(i));

            file.WriteLine(line.ToString());

            count++;
          }
        }

        file.Close();
        conn.Close();

        // influx.exe -database <name> -import -path=<filename> -precision=s
        Process.Start(
          Path.Combine(influx_home, "influx.exe"),
          $"-database '{influx_dbname}' -import -path={import_filename} -precision=s"
        );
      }

      #region HTTP API
      //var client = new WebClient();
      //client.UploadData("http://localhost:8086/write?db=perf_lhmi", File.ReadAllBytes(filename));

      /*var req = WebRequest.Create("http://localhost:8086/write?db=perf_lhmi");
      req.Method = "POST";
      using (var s = req.GetRequestStream())
      {
        var data = File.ReadAllBytes(filename);
        s.Write(data, 0, data.Length);
      }

      var rsp = req.GetResponse();*/
      #endregion
    }
  }

  // https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_tutorial/
  class InfluxLine
  {
    public InfluxLine()
    {
      fields = new Dictionary<string, object>();
    }

    public string measurement;
    public Dictionary<string, string> tags;
    public Dictionary<string, object> fields;
    public DateTimeOffset? timestamp;

    public override string ToString()
    {
      Debug.Assert(fields != null, "fields must be set");

      var result = new StringBuilder(measurement);
      if (tags != null)
        foreach (var p in tags)
          result.AppendFormat($",{p.Key}={p.Value}");
      result.Append(" ");
      foreach (var p in fields)
        result.Append($"{p.Key}={p.Value},");
      result.Length -= 1;
      if (timestamp != null)
      {
        result.Append(" ");
        result.AppendFormat("{0}", timestamp.Value.ToUniversalTime().ToUnixTimeSeconds());
      }

      return result.ToString();
    }
  }
}
