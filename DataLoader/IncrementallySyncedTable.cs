using System.Threading;
using DataLoader.Destination;
using DataLoader.Source;
using Serilog;

namespace DataLoader
{
    public class IncrementallySyncedTable<T1,T2>: ISyncedTable<T1, T2>
    {
        private readonly IIncrementalSource<byte[], T1> _source;
        private readonly IIncrementalDestination<byte[], T2> _destination;
        private readonly IMapper<T1, T2> _mapper;

        public IncrementallySyncedTable(IIncrementalSource<byte[], T1> source, IIncrementalDestination<byte[], T2> destination, IMapper<T1,T2> mapper)
        {
            _source = source;
            _destination = destination;
            _mapper = mapper;
        }

        public int Sync(CancellationToken token = default)
        {
            Log.Debug("Updating {0} with data from {1}", _destination.Name, _source.Name);
            
            var lastDestinationKey = _destination.GetLastKey(token);
            var data = _source.GetRows(keyFrom: lastDestinationKey);

            var mappedData = _mapper.Map(data);

            var succeeded = _destination.Save(mappedData, token);

            Log.Verbose("{0} rows were inserted", succeeded);
            return succeeded;
        }
    }
}
