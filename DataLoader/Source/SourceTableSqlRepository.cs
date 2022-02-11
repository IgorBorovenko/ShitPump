using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Dapper;

namespace DataLoader.Source
{
    //TODO: inject RowVersionBatchSplitter
    public class SourceTableSqlRepository<T> : RowVersionBatchSplitter<T>, IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly int _commandTimeout;
        private readonly TableAttribute _table;
        private readonly IEnumerable<ColumnAttribute> _columns;
        private readonly RowVersionColumnAttribute _rowVersionColumn;
        private readonly static byte[] minRowVersion = new byte[] { 0 };
        private readonly static byte[] maxRowVersion = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

        public override string Name => _table.Name;
        
        public SourceTableSqlRepository(string connectionString, int commandTimeout = 60, SplitterOptions options = default)
            :base(options)
        {
            try
            {
                _table = typeof(T).GetTableAttributeFromClass();
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"{typeof(T)} must be decorated with {typeof(TableAttribute)}", e);
            }

            try
            {
                _columns = typeof(T).GetColumnAttributesFromClass();
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"{typeof(T)} must have at least one property decorated with {typeof(ColumnAttribute)}", e);
            }

            try
            {
                _rowVersionColumn = _columns.OfType<RowVersionColumnAttribute>().Single();
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"{typeof(T)} must have one property decorated with {typeof(RowVersionColumnAttribute)}", e);
            }

            _connection = new SqlConnection(connectionString);
            _connection.Open();
            _commandTimeout = commandTimeout;
        }

        public override IEnumerable<T> GetRows(byte[] rowVersionFrom = default, byte[] rowVersionTo = default, CancellationToken token = default)
        {
            rowVersionFrom = rowVersionFrom == default ? minRowVersion : rowVersionFrom;
            rowVersionTo = rowVersionTo == default ? maxRowVersion : rowVersionTo;

            var columnsClause = string.Join(",", _columns.Select(x => x.Name));
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
