using System;

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
            var conn = Environment.GetEnvironmentVariable("AZSTORE__CONNECTION");
              
            var tab = new AzureTable<TestEntity>("testtable", conn );
            var r= tab.InsertOrReplaceAsync(new TestEntity() {Name = "colin", Type = 2, When = DateTimeOffset.Now}).ConfigureAwait(false).GetAwaiter().GetResult();
            if ( r != Result.Ok)
            {
                Console.WriteLine("Error: {0}", r);
            }
            else
            {
                Console.WriteLine("==OK==");
            }
        }
    }
}
