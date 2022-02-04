using System.Collections.Generic;
using System.Threading;

namespace DataLoader.Destination
{
    public interface IDestination<TValue>
    {
        string Name { get; }
        int Save(IEnumerable<TValue> data, CancellationToken token);
    }

    public interface IClearableDestination<TValue>: IDestination<TValue>
    {
        void Clear(CancellationToken token);
    }

    public interface IIncrementalDestination<TKey, TValue> : IDestination<TValue>
    {
        TKey GetLastKey(CancellationToken token);
    }

    public interface IBatchableDestination<TKey, TValue> : IIncrementalDestination<TKey, TValue>
    {
        int DeleteUnsuccessfulBatches(CancellationToken token);
    }
}