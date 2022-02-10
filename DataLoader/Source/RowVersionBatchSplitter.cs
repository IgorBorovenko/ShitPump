using System;
using System.Collections.Generic;
using System.Threading;
using Serilog;

namespace DataLoader.Source
{
    public abstract class RowVersionBatchSplitter<T> : IBatchableSource<byte[], T>
    {
        public abstract string Name { get; }
        private readonly SplitterOptions _options;

        protected RowVersionBatchSplitter(SplitterOptions options)
        {
            _options = options ?? SplitterOptions.Default;
        }

        public IEnumerable<IEnumerable<T>> GetBatches(byte[] rowVersionFrom, byte[] rowVersionTo = default, CancellationToken token = default)
        {
            byte[] fromRowVersion;
            var isFullLoad = false;

            var sourceMaxRowVersion = rowVersionTo ?? GetMaxRowVersion(token);
            if (sourceMaxRowVersion is null || sourceMaxRowVersion.Length == 0)
            {
                Log.Debug("Data source is empty");
                yield break;
            }

            if (rowVersionFrom is null || rowVersionFrom.Length == 0)
            {
                //means this is initial load, expect huge number of rows
                fromRowVersion = GetMinRowVersion(token);
                fromRowVersion = UlongToRowVersion(RowVersionToUlong(fromRowVersion) - 1);
                isFullLoad = true;
            }
            else
                fromRowVersion = rowVersionFrom;

            var from = RowVersionToUlong(fromRowVersion);
            var to = RowVersionToUlong(sourceMaxRowVersion);

            if (isFullLoad)
            {
                Log.Debug("Initial loading rows between source min RowVersion = \"{0}\" and source max RowVersion = \"{1}\" in batches by {2} RowVersions", ByteArrayToString(fromRowVersion), ByteArrayToString(sourceMaxRowVersion), _options.FullLoadingBatchIncrement);

                var batchStart = from;
                var batchEnd = batchStart + _options.FullLoadingBatchIncrement > to ? to : batchStart + _options.FullLoadingBatchIncrement;
                var i = 0;
                while (batchEnd <= to)
                {
                    var rvFrom = UlongToRowVersion(batchStart);
                    var rvTo = UlongToRowVersion(batchEnd);
                    Log.Verbose("Querying batch #{0}, rows between RowVersions \"{1}\" and \"{2}\"", i, ByteArrayToString(rvFrom), ByteArrayToString(rvTo));
                    yield return GetRows(rvFrom, rvTo, token);
                    batchStart += _options.FullLoadingBatchIncrement;
                    batchEnd += _options.FullLoadingBatchIncrement;
                    i++;
                }
            }
            else
            {
                var batchIncrement = (to - from) / _options.BatchingFactor;

                if (batchIncrement <= _options.IncrementalLoadingBatchIncrement)
                {
                    Log.Debug("Incremental loading rows between max known RowVersion = \"{0}\" and source max RowVersion = \"{1}\" in {2} batches",
                        ByteArrayToString(fromRowVersion), ByteArrayToString(sourceMaxRowVersion), _options.BatchingFactor);

                    var batchStart = from;
                    for (var i = 0; i < _options.BatchingFactor; i++)
                    {
                        var rvFrom = UlongToRowVersion(batchStart);
                        var rvTo = UlongToRowVersion(batchStart + batchIncrement);
                        Log.Verbose("Querying batch #{0}, rows between RowVersions \"{1}\" and \"{2}\"", i, ByteArrayToString(rvFrom), ByteArrayToString(rvTo));
                        yield return GetRows(rvFrom, rvTo, token);
                        batchStart += batchIncrement;
                    }
                }
                else
                {
                    Log.Debug("Incremental loading rows between max known RowVersion = \"{0}\" and source max RowVersion = \"{1}\" in batches by {2} RowVersions",
                        ByteArrayToString(fromRowVersion), ByteArrayToString(sourceMaxRowVersion), _options.IncrementalLoadingBatchIncrement);

                    var batchStart = from;
                    var batchEnd = batchStart + _options.IncrementalLoadingBatchIncrement;
                    var i = 0;
                    while (batchEnd < to)
                    {
                        var rvFrom = UlongToRowVersion(batchStart);
                        var rvTo = UlongToRowVersion(batchEnd);
                        Log.Verbose("Querying batch #{0}, rows between RowVersions \"{1}\" and \"{2}\"", i, ByteArrayToString(rvFrom), ByteArrayToString(rvTo));
                        yield return GetRows(rvFrom, rvTo, token);
                        batchStart += _options.IncrementalLoadingBatchIncrement;
                        batchEnd += _options.IncrementalLoadingBatchIncrement;
                        i++;
                    }
                }
            }
        }
        
        public IEnumerable<T> GetRows(CancellationToken token) => GetRows(default(byte[]), default(byte[]), token);

        public byte[] GetLastKey(CancellationToken token) => GetMaxRowVersion(token);
        
        public abstract IEnumerable<T> GetRows(byte[] rowVersionFrom, byte[] rowVersionTo, CancellationToken token);

        protected abstract byte[] GetMinRowVersion(CancellationToken token);

        protected abstract byte[] GetMaxRowVersion(CancellationToken token);

        private static ulong RowVersionToUlong(byte[] rowVersion)
        {
            if (!BitConverter.IsLittleEndian)
                return BitConverter.ToUInt64(rowVersion, 0);

            var clone = (byte[])rowVersion.Clone();
            Array.Reverse(clone);
            return BitConverter.ToUInt64(clone, 0);
        }

        private static byte[] UlongToRowVersion(ulong rowVersion)
        {
            var bytes = BitConverter.GetBytes(rowVersion);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        private static string ByteArrayToString(byte[] rowVersion)
        {
            string retVal = "0x";
            string toAdd = "";
            foreach (byte b in rowVersion)
            {
                toAdd = b.ToString("X");
                if (toAdd.Length == 1)
                {
                    retVal += "0" + toAdd;
                }
                else
                {
                    retVal += toAdd;
                }
            }
            return retVal;
        }
    }
    
    public class SplitterOptions
    {
        public uint BatchingFactor;
        public uint FullLoadingBatchIncrement;
        public uint IncrementalLoadingBatchIncrement;

        public static SplitterOptions Default = new SplitterOptions { BatchingFactor = 50, FullLoadingBatchIncrement = 100_000_000, IncrementalLoadingBatchIncrement = 1_000_000_000 };
    }
}
