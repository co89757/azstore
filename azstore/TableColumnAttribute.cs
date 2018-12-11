using System;

namespace azstore
{
    [AttributeUsage(AttributeTargets.Property, Inherited = true)]
    public class TableColumnAttribute : Attribute
    {
        public string Name { get; }

        public TableColumnAttribute(string name)
        {
            Name = name;
        }
    }
}