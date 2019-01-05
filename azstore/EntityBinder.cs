using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
namespace azstore {
    public class EntityBinder<T> where T : class, IDataEntity, new() {
        public virtual DynamicTableEntity Write(T data) {
            if (data == null)
                return null;
            DynamicTableEntity e = new DynamicTableEntity(data.GetPartitionKey(), data.GetRowKey());
            foreach (var propInfo in typeof(T).GetProperties()) {
                var tblColumnAttr = propInfo.GetCustomAttribute<TableColumnAttribute>();
                if (tblColumnAttr != null) {
                    var columnName = tblColumnAttr.Name;
                    var val = propInfo.GetValue(data);
                    if (propInfo.PropertyType.IsEnum) {
                        var intval = (int)val;
                        e.Properties[columnName] = EntityProperty.GeneratePropertyForInt(intval);
                    } else {
                        e.Properties[columnName] = EntityProperty.CreateEntityPropertyFromObject(val);
                    }
                }
            }

            return e;
        }

        public virtual T Read(DynamicTableEntity e) {
            if (e == null) {
                return null;
            }

            T data = new T();
            foreach (var propInfo in typeof(T).GetProperties()) {
                var tblColumnAttr = propInfo.GetCustomAttribute<TableColumnAttribute>();
                if (tblColumnAttr != null) {
                    var columnName = tblColumnAttr.Name;
                    if (e.Properties.TryGetValue(columnName, out EntityProperty o)) {
                        var pt = propInfo.PropertyType;
                        if (pt.FullName.Equals("System.DateTimeOffset")) {
                            propInfo.SetValue(data, o.DateTimeOffsetValue);
                        } else if (pt.IsEnum) {

                            var v = Enum.Parse(pt, o.PropertyAsObject.ToString(), true);
                            propInfo.SetValue(data, v);

                        } else
                            propInfo.SetValue(data, o.PropertyAsObject);
                    }
                }
            }
            return data;
        }
    }
}