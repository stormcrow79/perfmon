using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace collector
{
  class Program
  {
    static void Main(string[] args)
    {
      while (true)
      {
        var timer = Stopwatch.StartNew();

        foreach (var categoryName in new[] { ".NET CLR Memory", "Process" })
        {
          var refCat = category.FirstOrDefault(c => c.Name == categoryName);
          var cat = new PerformanceCounterCategory(categoryName);
          foreach (var counter in cat.GetCounters("KarismaServer"))
          {
            if (!refCat.Counter.Contains(counter.CounterName)) continue;
            Console.WriteLine($"\t{counter.CategoryName}\\{counter.CounterName}({counter.InstanceName})\t{(long)counter.NextValue()}");
          }
        }

        timer.Stop();
        Console.WriteLine(timer.Elapsed);

        Thread.Sleep(15000);
      }
    }

    static Category[] category = new[]
    {
      new Category()
      {
        Name = ".NET CLR Memory",
        Instance = new[] { "KarismaServer" },
        Counter = new []
        {
          "# Bytes in all Heaps",
          "# Gen 0 Collections",
          "# Gen 1 Collections",
          "# Gen 2 Collections",
          "% Time in GC",
          "Gen 0 heap size",
          "Gen 1 heap size",
          "Gen 2 heap size",
          "Large Object Heap size",
          "Process ID"
        }
      },
      new Category()
      {
        Name = "Process",
        Instance = new[] { "KarismaServer" },
        Counter = new []
        {
          "% Processor Time",
          "Elapsed Time",
          "Handle Count",
          "ID Process",
          "Page File Bytes",
          "Private Bytes",
          "Thread Count",
          "Virtual Bytes",
          "Working Set - Private"
        }
      }
    };
  }

  class Category
  {
    public string Name;
    public string[] Instance;
    public string[] Counter;
  }
}
