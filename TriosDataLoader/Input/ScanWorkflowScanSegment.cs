using System;
using DataLoader;

namespace TriosDataLoader.Input
{
    [Table("[DentalDesktopTRIOS].[ScanWorkflowScanSegment]")]
    public class ScanWorkflowScanSegment: ITableRecord
    {
        [Column("ScanWorkflowScanSegmentID")]
        public Guid ScanWorkflowScanSegmentID { get; set; }

        [Column("ScanWorkflowScanID")]
        public Guid ScanWorkflowScanID { get; set; }

        [Column("ScannerSerialNumber")]
        public string ScannerSerialNumber { get; set; }

        [RowVersionColumn("RowVersion")]
        public byte[] RowVersion { get; set; }
    }
}
