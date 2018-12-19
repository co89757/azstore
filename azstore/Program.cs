using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
namespace azstore
{
    class Program
    {
        public class TestEntity : IDataEntity
        {
            [TableColumn("Name")]
            public string Name { get; set; }

            [TableColumn("Type")]
            public int Type { get; set; }
            [TableColumn("When")]
            public DateTimeOffset When { get; set; }
            [TableColumn("Age")]    
            public int Age { get; set; }
            public string GetRowKey()
            {
                return Name;
            }

            public string GetPartitionKey()
            {
                return Type.ToString();
            }
        }


        static void Main(string[] args)
        {
            var cfg = new ConfigurationBuilder()
            .SetBasePath(System.IO.Directory.GetCurrentDirectory())
            .AddJsonFile("./appsettings.json")
            .Build();
            var conn = cfg.GetSection("azure")["connection"];
            var tab = new AzureTable<TestEntity>("testtable", conn );
            string rk = "129f01a8-7528-4f05-b3aa-d05c78bbb26a";
            string pk = "4";
            Dictionary<string,object> props = new Dictionary<string, object>(){
                {"Age", 50}
            };
            var res = tab.MergeOne(pk, rk, props).ConfigureAwait(false).GetAwaiter().GetResult();

             
            System.Console.WriteLine($"result: {res.HttpStatus} "); 
        }
    }
}
