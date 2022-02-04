using System.Threading;
using DataLoader.Destination;
using DataLoader.Source;
using Serilog;

namespace DataLoader
{
    public class RowVersionSyncedTable<T1,T2>: ISyncedTable<T1, T2>
    {
        private readonly IBatchableSource<byte[], T1> _source;
        private readonly IBatchableDestination<byte[], T2> _destination;
        private readonly IMapper<T1, T2> _mapper;

        public RowVersionSyncedTable(IBatchableSource<byte[], T1> source, IBatchableDestination<byte[], T2> destination, IMapper<T1,T2> mapper)
        {
            _source = source;
            _destination = destination;
            _mapper = mapper;
        }

        public int Sync(CancellationToken token = default)
        {
            Log.Verbose("Updating {0} with data from {1}", _destination.Name, _source.Name);
            
            var deleted = _destination.DeleteUnsuccessfulBatches(token);
            Log.Verbose("Deleted \"unsuccessful\" batches: {0} ", deleted);

            var lastDestinationRowVersion = _destination.GetLastKey(token);
            var batches = _source.GetBatches(keyFrom: lastDestinationRowVersion);
            
            var succeeded = 0;
            foreach (var batch in batches)
            {
                var mappedBatch = _mapper.Map(batch);

                var rows = _destination.Save(mappedBatch, token);

                Log.Verbose("Batch of {0} rows was inserted", rows);
                succeeded += rows;
            }
            
            Log.Verbose("{0} rows were inserted", succeeded);
            return succeeded;
        }
    }
}
