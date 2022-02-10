using System.Threading;
using DataLoader.Destination;
using DataLoader.Source;
using Serilog;

namespace DataLoader
{
    public class FullLoadSyncedTable<T1,T2>: ISyncedTable<T1, T2>
    {
        private readonly ISource<T1> _source;
        private readonly IClearableDestination<T2> _destination;
        private readonly IMapper<T1, T2> _mapper;

        public FullLoadSyncedTable(ISource<T1> source, IClearableDestination<T2> destination, IMapper<T1,T2> mapper)
        {
            _source = source;
            _destination = destination;
            _mapper = mapper;
        }

        public int Sync(CancellationToken token = default)
        {
            Log.Debug("Full loading into {0} data from {1}", _destination.Name, _source.Name);
            
            _destination.Clear(token);
            Log.Verbose("Destination cleared");

            var data = _source.GetRows(token);

            var mappedData = _mapper.Map(data);

            var succeeded = _destination.Save(mappedData, token);
            Log.Verbose("{0} rows were inserted", succeeded);

            return succeeded;
        }
    }
}
