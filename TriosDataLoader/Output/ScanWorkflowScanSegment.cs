using System;
using DataLoader;

namespace TriosDataLoader.Output
{
    [Table("[Source].[DentalDesktopTRIOS.ScanWorkflowScanSegment]")]
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
        
        [WholeLoadSucceededColumn]
        public int WholeLoadSucceeded { get; set; }
    }
}