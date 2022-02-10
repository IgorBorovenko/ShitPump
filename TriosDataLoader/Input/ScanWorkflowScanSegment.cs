using System;
using DataLoader;

namespace TriosDataLoader.Input
{
    [Table("[DentalDesktopTRIOS].[ScanWorkflowScanSegment]")]
    public class ScanWorkflowScanSegment
    {
        [Column]
        public Guid ScanWorkflowScanSegmentID { get; set; }

        [Column]
        public Guid ScanWorkflowScanID { get; set; }

        [Column]
        public string ScannerSerialNumber { get; set; }

        [RowVersionColumn]
        public byte[] RowVersion { get; set; }
    }
}
