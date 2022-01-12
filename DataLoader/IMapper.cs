using System.Collections.Generic;
using System.Linq;


namespace DataLoader
{
    public interface IMapper<T1, T2>
    {
        T2 Map(T1 obj);
        IEnumerable<T2> Map(IEnumerable<T1> obj);
    }

    public class DefaultMapper<T1, T2> : IMapper<T1, T2>
    {
        private readonly AutoMapper.IMapper _autoMapper;

        public DefaultMapper(AutoMapper.IMapper autoMapper)
        {
            _autoMapper = autoMapper;
        }

        public T2 Map(T1 obj)
        {
            return _autoMapper.Map<T2>(obj);
        }

        public IEnumerable<T2> Map(IEnumerable<T1> obj)
        {
            return obj.Select(Map);
        }
    }
}