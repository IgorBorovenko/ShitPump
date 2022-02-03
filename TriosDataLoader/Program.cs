using System;
using System.Diagnostics;
using Castle.Windsor;
using DataLoader;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using Serilog.Exceptions;

namespace TriosDataLoader
{
    class Program
    {
        static void Main(string[] args)
        {
            var loggerConfiguration = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .Enrich.FromLogContext()
                .Enrich.WithProperty("Context", "STAT")
                .Enrich.WithProperty("Service", "TriosDataLoader")
                .Enrich.WithProperty("SessionId", Guid.NewGuid())
                .Enrich.With<DemystifiedExceptionStackTraceEnricher>()
                .Enrich.WithExceptionDetails()
                .WriteTo.Async(x => x.Console())
                .WriteTo.Async(x => x.File(@"logs/.txt", rollingInterval: RollingInterval.Day));
            Log.Logger = loggerConfiguration.CreateLogger();
            
            Log.Information("Process ID: {0}", Process.GetCurrentProcess().Id);

            var configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json", false).Build();

            

            try
            {
                var container = new WindsorContainer();
                container.Install(
                    new WindsorInstaller(configuration),
                    new AutoMapperInstaller(configuration),
                    new ScanWorkflowScanSegmentInstaller(configuration)
                );

                DoJob(container);
            }
            catch (Exception e)
            {
                Log.Fatal("Trios data loading failed.", e);
                throw;
            }
        }

        private static void DoJob(WindsorContainer container)
        {
            Log.Debug("Syncing ScanWorkflowScanSegment:");
            var sw = Stopwatch.StartNew();
            
            var table = container.Resolve<ISyncedTable<Input.ScanWorkflowScanSegment, Output.ScanWorkflowScanSegment>>();
            var copiedRows = table.Sync();
            
            Log.Debug("Total copied rows: {0}, time took: {1}", copiedRows, sw.Elapsed);
        }

        class DemystifiedExceptionStackTraceEnricher : ILogEventEnricher
        {
            public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
            {
                logEvent.Exception?.Demystify();
            }
        }
    }
}
