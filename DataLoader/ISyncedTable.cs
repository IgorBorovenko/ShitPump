using System.Threading;

namespace DataLoader
{
    public interface ISyncedTable<T1, T2>
    {
        int Sync(CancellationToken token = default);
    }
}
