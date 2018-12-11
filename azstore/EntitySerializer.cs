using Microsoft.WindowsAzure.Storage.Table;
using System.Reflection;
using System.Collections.Generic;
using System.Text;
using System;
namespace azstore
{
    public class EntityBinder<T> where T : IDataEntity, new()
    {
        public virtual DynamicTableEntity Write(T data)
        {
            DynamicTableEntity e = new DynamicTableEntity(data.GetPartitionKey(), data.GetRowKey());
            foreach (var propInfo in typeof(T).GetProperties())
            {
                var tblColumnAttr = propInfo.GetCustomAttribute<TableColumnAttribute>();
                if (tblColumnAttr != null)
                {
                    var columnName = tblColumnAttr.Name;
                    var val = propInfo.GetValue(data);
                    e.Properties[columnName] = EntityProperty.CreateEntityPropertyFromObject(val);
                }
            }

            return e;
        }
      
        public virtual T Read(DynamicTableEntity e)
        {
            if (e == null)
            {
                throw new ArgumentNullException("entity");
            }

            T data = new T();
            foreach (var propInfo in typeof(T).GetProperties())
            {
                var tblColumnAttr = propInfo.GetCustomAttribute<TableColumnAttribute>();
                if (tblColumnAttr != null)
                {
                    var columnName = tblColumnAttr.Name;
                    if (e.Properties.TryGetValue(columnName, out EntityProperty o))
                    {
                        propInfo.SetValue(data, o.PropertyAsObject);
                    }
                }
            }
            return data;
        }
         
    }
}
