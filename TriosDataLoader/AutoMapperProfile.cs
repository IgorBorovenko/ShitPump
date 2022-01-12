using AutoMapper;

namespace TriosDataLoader
{
    public class AutoMapperProfile : Profile
    {
        public AutoMapperProfile()
        {
            CreateMap<Input.ScanWorkflowScanSegment, Output.ScanWorkflowScanSegment>();
        }
    }
}