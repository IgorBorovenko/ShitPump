using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using DataLoader;
using DataLoader.Destination;
using DataLoader.Source;
using Microsoft.Extensions.Configuration;

namespace TriosDataLoader
{
    public class ScanWorkflowScanSegmentInstaller : IWindsorInstaller
    {
        private readonly IConfiguration _config;

        public ScanWorkflowScanSegmentInstaller(IConfiguration config)
        {
            _config = config;
        }

        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(
                Component.For<IBatchableSource<byte[], Input.ScanWorkflowScanSegment>>().ImplementedBy<SourceTableSqlRepository<Input.ScanWorkflowScanSegment>>()
                    .DependsOn(new { connectionString  = _config.GetSection("source")["connectionString"], commandTimeout = int.Parse(_config.GetSection("source")["commandTimeout"]) }),
                Component.For<IBatchableDestination<byte[], Output.ScanWorkflowScanSegment>>().ImplementedBy<DestinationTableSqlRepository<Output.ScanWorkflowScanSegment>>()
                    .DependsOn(new { connectionString = _config.GetSection("destination")["connectionString"], commandTimeout = int.Parse(_config.GetSection("destination")["commandTimeout"]) }),
                Component.For<IMapper<Input.ScanWorkflowScanSegment, Output.ScanWorkflowScanSegment>>().ImplementedBy<DefaultMapper<Input.ScanWorkflowScanSegment, Output.ScanWorkflowScanSegment>>(),
                Component.For<ISyncedTable<Input.ScanWorkflowScanSegment, Output.ScanWorkflowScanSegment>>().ImplementedBy<RowVersionSyncedTable<Input.ScanWorkflowScanSegment, Output.ScanWorkflowScanSegment>>()
            );
        }
    }
}