using System;
using System.Collections.Generic;
using System.Linq;

namespace DataLoader
{
    public interface ITableRecord { }

    public static class TableRecordExtensions
    {
        public static TableAttribute GetTableAttributeFromClass(this Type type)  => 
            (TableAttribute)type.GetCustomAttributes(typeof(TableAttribute), true).Single();

        public static IEnumerable<ColumnAttribute> GetColumnAttributesFromClass(this Type type) =>
            type.GetProperties().Select(x => (ColumnAttribute)x.GetCustomAttributes(typeof(ColumnAttribute), true).First());
    }

    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public string Name { get; }

        public TableAttribute(string name)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; }
        public int Order { get; }

        public ColumnAttribute(string name, int order = 0)
        {
            Name = name;
            Order = order;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class RowVersionColumnAttribute : ColumnAttribute
    {
        public RowVersionColumnAttribute(string name, int order = 0)
            :base(name, order)
        {
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class WholeLoadSucceededColumnAttribute : ColumnAttribute
    {
        public WholeLoadSucceededColumnAttribute(string name, int order = 0)
            : base(name, order)
        {
        }
    }

}
