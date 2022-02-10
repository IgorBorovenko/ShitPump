using System.Collections.Generic;
using System.Threading;

namespace DataLoader.Source
{
    public interface ISource<TValue>
    {
        string Name { get; }
        IEnumerable<TValue> GetRows(CancellationToken token);
    }

    public interface IIncrementalSource<TKey, TValue> : ISource<TValue>
    {
        TKey GetLastKey(CancellationToken token);
        IEnumerable<TValue> GetRows(TKey keyFrom, TKey keyTo = default, CancellationToken token = default);
    }

    public interface IBatchableSource<TKey, TValue> : IIncrementalSource<TKey, TValue>
    {
        IEnumerable<IEnumerable<TValue>> GetBatches(TKey keyFrom, TKey keyTo = default, CancellationToken token = default);
    }
}