using System;
using System.Collections.Generic;
using System.Text;

namespace azstore
{
    public interface IDataEntity
    {
          string GetRowKey();
          string GetPartitionKey();
    }
}
