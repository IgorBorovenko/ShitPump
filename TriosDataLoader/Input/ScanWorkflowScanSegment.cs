using System;
using DataLoader;

namespace TriosDataLoader.Input
{
    [Table("[DentalDesktopTRIOS].[ScanWorkflowScanSegment]")]
    public class ScanWorkflowScanSegment: ITableRecord
    {
        [Column("[ScanWorkflowScanSegmentID]", 0)]
        public Guid ScanWorkflowScanSegmentID { get; set; }

        [Column("[ScanWorkflowScanID]", 1)]
        public Guid ScanWorkflowScanID { get; set; }

        [Column("[ScannerSerialNumber]", 2)]
        public string ScannerSerialNumber { get; set; }

        [RowVersionColumn("[RowVersion]", 3)]
        public byte[] RowVersion { get; set; }
    }
}
