using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace sql2es
{
  class Program
  {
    static void Main(string[] args)
    {
      var cb = new SqlConnectionStringBuilder();
      cb.DataSource = @"gavinm\std17";
      cb.InitialCatalog = "perf_lhmi_2";
      cb.IntegratedSecurity = true;

      var conn = new SqlConnection(cb.ToString());
      conn.Open();
      var cmd = conn.CreateCommand();
      cmd.CommandText = "select operation_id, machine_name, signature, timestamp, component, total_duration, total_size, user_name from [operations];";
      var reader = cmd.ExecuteReader();

      var i = 0L;
      var filename = $"{i}.txt";
      var output = new StreamWriter(filename);
      output.NewLine = "\n";

      var client = new WebClient();
      client.BaseAddress = "http://localhost:9200";
      var tasks = new List<Task>();

      var js = new JsonSerializerSettings()
      {
        NullValueHandling = NullValueHandling.Ignore
      };


      var timer = Stopwatch.StartNew();

      while (reader.Read())
      {
        var timestamp = (DateTime)reader[3];

        var op = new
        {
          operation_id = (long)reader[0],
          machine_name = (string)reader[1],
          signature = (string)reader[2],
          timestamp = timestamp.ToString("yyyy-MM-dd'T'HH:mm:ss.fff"),
          component = (string)reader[4],
          total_duration = (double)reader[5],
          total_size = (long)reader[6],
          user_name = (string)reader[7]
        };

        var header = new Header()
        {
          index = new Header.Meta() { _index = $"lhmi_{timestamp:yyyyMMdd}", _type = "operation", _id = op.operation_id.ToString() }
        };

        output.WriteLine(JsonConvert.SerializeObject(header, js));
        output.WriteLine(JsonConvert.SerializeObject(op, js));

        i++;

        if (i % 10000 == 0)
        {
          output.Close();
          var uploadFile = filename;

          //Console.WriteLine($"upload start: {uploadFile}");
          //client.Headers[HttpRequestHeader.ContentType] = "application/x-ndjson";
          //var t = client.UploadFileTaskAsync("/_bulk", uploadFile);
          //  t.ContinueWith(_ => Console.WriteLine($"upload done:  {uploadFile}"));
          //tasks.Add(t);
          // todo: ContinueWith delete

          //client.UploadFile("_bulk", uploadFile);

          var req = WebRequest.Create("http://localhost:9200/_bulk");
          req.Method = "POST";
          req.ContentType = "application/x-ndjson";
          var s = req.GetRequestStream();
          var b = File.ReadAllBytes(uploadFile);
          s.Write(b, 0, b.Length);
          var rsp = req.GetResponse();

          filename = $"{i}.txt";
          output = new StreamWriter(filename);
          output.NewLine = "\n";
        }
      }

      Console.WriteLine("\nfinishing up ...");
      foreach (var t in tasks)
        t.Wait();
      Console.WriteLine("\ndone!");
    }
  }

  class Header
  {
    public Meta index;
    public Meta delete;
    public Meta create;
    public Meta update;
    public class Meta
    {
      public string _index;
      public string _type;
      public string _id;
    }
  }
}
