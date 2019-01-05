using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.EnvironmentVariables;
namespace azstore {
    class Program {

        public enum JType {
            Ok,
            Fail,
            Nil
        }

        public class TestEntity : IDataEntity {
            [TableColumn("Name")]
            public string Name { get; set; }

            [TableColumn("Type")]
            public int Type { get; set; }

            [TableColumn("When")]
            public DateTimeOffset When { get; set; }

            [TableColumn("JobType")]
            public JType JobType { get; set; }

            [TableColumn("Age")]
            public int Age { get; set; }
            
            public string  RowKey { get => Name; set => Name = value; }
            public string  PartitionKey { get => Type.ToString(); set => Type = int.Parse(value); }
        }

        static void Main(string[] args) {
            var cfg = new ConfigurationBuilder()
                .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                .AddJsonFile("./appsettings.json")
                .AddEnvironmentVariables()
                .Build();
            var conn = cfg.GetSection("azure")["connection"];
            var tab = new AzureTable<TestEntity>("testtable", conn);
            var eid = Guid.NewGuid().ToString();
            var pk = 2;
            var testent = new TestEntity {
                Name = eid,
                Age = 30,
                Type = pk,
                JobType = JType.Fail,
                When = DateTimeOffset.UtcNow
            };

            var rk = "038e1d11-2a1f-4fa1-9485-29e418dce8b0";
            var result = tab.RetrieveOnePartial<int?>("2",rk, (prk, rowk, ts, props, etag)=> props.ContainsKey("Age")? props["Age"].Int32Value : 0  ).GetAwaiter().GetResult();

            System.Console.WriteLine($"insert result: {result.HttpStatus} result data: {result.Data} ");

            var entback = tab.RetrieveOne(pk.ToString(), eid).GetAwaiter().GetResult().Data;
            System.Console.WriteLine($"returned entity, jtype = {entback.JobType}");
        }
    }
}