using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Dapper;

namespace DataLoader.Source
{
    public class SourceTableSqlRepository<T> : RowVersionBatchSplitter<T>, IDisposable where T: ITableRecord
    {
        private readonly SqlConnection _connection;
        private readonly int _commandTimeout;
        private readonly TableAttribute _table = typeof(T).GetTableAttributeFromClass();
        private readonly IEnumerable<ColumnAttribute> _columns = typeof(T).GetColumnAttributesFromClass();
        private readonly RowVersionColumnAttribute _rowVersionColumn = typeof(T).GetColumnAttributesFromClass().OfType<RowVersionColumnAttribute>().Single();

        public override string Name => _table.Name;

        public SourceTableSqlRepository(string connectionString, int commandTimeout = 60, SplitterOptions options = default)
            :base(options)
        {
            _connection = new SqlConnection(connectionString);
            _connection.Open();
            _commandTimeout = commandTimeout;
        }
        
        public override IEnumerable<T> GetRows(byte[] rowVersionFrom, byte[] rowVersionTo, CancellationToken token)
        {
            var columnsClause = string.Join(",", _columns.OrderBy(x => x.Order).Select(x => x.Name));
            var sql = $"SELECT {columnsClause} FROM {_table.Name} WHERE @RowVersionFrom < {_rowVersionColumn.Name} AND {_rowVersionColumn.Name} <= @RowVersionTo";

            var command = new CommandDefinition(sql,
                new { RowVersionFrom = rowVersionFrom, RowVersionTo = rowVersionTo },
                null,
                _commandTimeout,
                CommandType.Text,
                CommandFlags.None,
                token);

            return _connection.Query<T>(command);
        }

        protected override byte[] GetMinRowVersion(CancellationToken token)
        {
            var sql = $"SELECT MIN({_rowVersionColumn.Name}) FROM {_table.Name}";
            var command = new CommandDefinition(sql,
                null,
                null,
                _commandTimeout,
                CommandType.Text,
                CommandFlags.None,
                token);

            return _connection.QuerySingleOrDefault<byte[]>(command);
        }

        protected override byte[] GetMaxRowVersion(CancellationToken token)
        {
            var sql = $"SELECT MAX({_rowVersionColumn.Name}) FROM {_table.Name}";
            var command = new CommandDefinition(sql,
                null,
                null,
                _commandTimeout,
                CommandType.Text,
                CommandFlags.None,
                token);

            return _connection.QuerySingleOrDefault<byte[]>(command);
        }

        public void Dispose()
        {
            _connection?.Dispose();
        }
    }
}
