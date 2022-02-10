using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using Dapper;

namespace DataLoader.Destination
{
    public class DestinationTableSqlRepository<T> : IBatchableDestination<byte[], T>, IIncrementalDestination<byte[], T>, 
        IClearableDestination<T>, IDisposable 
    {
        private readonly SqlConnection _connection;
        private readonly int _commandTimeout;
        private readonly TableAttribute _table;
        private readonly RowVersionColumnAttribute _rowVersionColumn;
        private readonly WholeLoadSucceededColumnAttribute _wholeLoadSucceededColumn;

        public int BulkBatchSize = 10000;
        public int BulkNotifyAfter = 10000;
        protected Func<T, object>[] Mappings { get; set; }
        public string Name => _table.Name;

        public DestinationTableSqlRepository(string connectionString, int commandTimeout = 60)
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
                _rowVersionColumn = typeof(T).GetColumnAttributesFromClass().OfType<RowVersionColumnAttribute>().Single();
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"{typeof(T)} must have one property decorated with {typeof(RowVersionColumnAttribute)}", e);
            }

            try
            {
                _wholeLoadSucceededColumn = typeof(T).GetColumnAttributesFromClass().OfType<WholeLoadSucceededColumnAttribute>().Single();
            }
            catch (InvalidOperationException e)
            {
                throw new InvalidOperationException($"{typeof(T)} must have one property decorated with {typeof(WholeLoadSucceededColumnAttribute)}", e);
            }

            _connection = new SqlConnection(connectionString);
            _connection.Open();
            _commandTimeout = commandTimeout;
            
            string sql = @"
SELECT [COLUMN_NAME]
FROM [INFORMATION_SCHEMA].[COLUMNS]
WHERE '[' + [TABLE_SCHEMA] + '].[' + [TABLE_NAME] + ']' = @Name
ORDER BY [ORDINAL_POSITION]";
            var columns = _connection.Query<string> (sql, new { Name = this.Name });
            Mappings = MakeGetters(columns);
        }

        private static Func<T, object>[] MakeGetters(IEnumerable<string> columns)
        {
            var properties = typeof(T).GetProperties().ToList();
            var mappings = new List<Func<T, object>>();

            foreach (var column in columns)
            {
                var property = properties.Single(x => x.GetCustomAttributes(typeof(ColumnAttribute), false).Cast<ColumnAttribute>().Single().Name == column);

                var input = Expression.Parameter(typeof(T), "input");
                var getProperty = Expression.Property(input, property);
                var castToObject = Expression.TypeAs(getProperty, typeof(object));
                var lambda = Expression.Lambda<Func<T, object>>(castToObject, input).Compile();
                
                mappings.Add(lambda);
            }

            return mappings.ToArray();
        }

        public virtual byte[] GetMaxRowVersion()
        {
            return _connection.QuerySingle<byte[]>($"SELECT MAX({_rowVersionColumn.Name}) FROM {Name}", null, null, _commandTimeout, CommandType.Text);
        }

        public int Save(IEnumerable<T> newUsages, CancellationToken token)
        {
            SqlTransaction transaction = null;
            try
            {
                transaction = _connection.BeginTransaction();

                var bulk = new SqlBulkCopy(_connection, SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.TableLock, transaction);
                bulk.DestinationTableName = Name;
                bulk.BatchSize = BulkBatchSize;
                bulk.BulkCopyTimeout = _commandTimeout;
                bulk.NotifyAfter = BulkNotifyAfter;
                bulk.SqlRowsCopied += (sender, args) => Serilog.Log.Verbose("{0} rows copied", args.RowsCopied);

                var sourceColumnIndex = 0;
                var destinationColumnIndex = 0;
                foreach (var mapping in Mappings)
                {
                    if (!(mapping is null))
                        bulk.ColumnMappings.Add(sourceColumnIndex, destinationColumnIndex);
                    sourceColumnIndex++;
                    destinationColumnIndex++;
                }

                var dataReaderWrapper = new DataReaderWrapper<T>(newUsages, Mappings);
                bulk.WriteToServer(dataReaderWrapper);

                if (dataReaderWrapper.RecordsAffected > 0)
                {
                    var sql = $"UPDATE {Name} SET {_wholeLoadSucceededColumn.Name} = 1 WHERE {_wholeLoadSucceededColumn.Name} = 0";
                    _connection.Execute(sql, null, transaction, _commandTimeout, CommandType.Text);
                }
                transaction?.Commit();
                
                return dataReaderWrapper.RecordsAffected;
            }
            catch
            {
                transaction?.Rollback();
                throw;
            }
        }

        public int DeleteUnsuccessfulBatches(CancellationToken token)
        {
            try
            {
                var sql = $"DELETE {Name} WHERE {_wholeLoadSucceededColumn.Name} = 0";
                var cmd = new CommandDefinition(sql, null, null, _commandTimeout, CommandType.Text, CommandFlags.None, token);
                var deleted = _connection.ExecuteScalar<int>(cmd);
                return deleted;
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public byte[] GetLastKey(CancellationToken token) => GetMaxRowVersion();

        public void Dispose()
        {
            _connection?.Dispose();
        }

        public void Clear(CancellationToken token)
        {
            string sql = $"TRUNCATE TABLE {Name}";
            var cmd = new CommandDefinition(sql, null, null, _commandTimeout, CommandType.Text, CommandFlags.None, token);
            _connection.Execute(cmd);
        }
    }
}