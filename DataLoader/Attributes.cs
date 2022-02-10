using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace DataLoader
{
    public static class AttributeExtensions
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
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException($"Argument cannot be null or empty", nameof(name));

            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; }

        public ColumnAttribute([CallerMemberName] string name = null)
        {
            Name = name;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class RowVersionColumnAttribute : ColumnAttribute
    {
        public RowVersionColumnAttribute([CallerMemberName] string name = null)
            :base(name)
        {
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class WholeLoadSucceededColumnAttribute : ColumnAttribute
    {
        public WholeLoadSucceededColumnAttribute([CallerMemberName] string name = null)
            : base(name)
        {
        }
    }

}
