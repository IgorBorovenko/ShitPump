using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Dapper;

namespace DataLoader.Destination
{
    public class DestinationTableSqlRepository<T> : IBatchableDestination<byte[], T>,IDisposable where T: ITableRecord
    {
        private readonly SqlConnection _connection;
        private readonly int _commandTimeout;
        private readonly TableAttribute _table = typeof(T).GetTableAttributeFromClass();
        private readonly RowVersionColumnAttribute _rowVersionColumn = typeof(T).GetColumnAttributesFromClass().OfType<RowVersionColumnAttribute>().Single();
        private readonly WholeLoadSucceededColumnAttribute _wholeLoadSucceededColumn = typeof(T).GetColumnAttributesFromClass().OfType<WholeLoadSucceededColumnAttribute>().Single();

        public int BulkBatchSize = 10000;
        public int BulkNotifyAfter = 10000;
        protected Func<T, object>[] Mappings { get; set; }
        public string Name => _table.Name;

        public DestinationTableSqlRepository(string connectionString, int commandTimeout = 60)
        {
            _connection = new SqlConnection(connectionString);
            _connection.Open();
            _commandTimeout = commandTimeout;
            
            string sql = @"
SELECT COLUMN_NAME, ORDINAL_POSITION 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' = @Name
ORDER BY ORDINAL_POSITION";
            var columns = _connection.Query<(string COLUMN_NAME, int ORDINAL_POSITION)> (sql, new { Name = this.Name });
            
            try
            {
                var properties = typeof(T).GetProperties().ToList();
                var mappings = new List<Func<T, object>>();
                foreach (var column in columns)
                {
                    var property = properties.Single(x => x.GetCustomAttributes(typeof(ColumnAttribute), false).OfType<ColumnAttribute>().Single().Name == column.COLUMN_NAME);
                    var getter = new Func<T, object>(x => property.GetValue(x)); //refactor with Expressions
                    mappings.Add(getter);
                }
                Mappings = mappings.ToArray();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
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
    }
}