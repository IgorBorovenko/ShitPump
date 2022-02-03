CREATE TABLE [DentalDesktopTRIOS].[ScanWorkflowScanSegment] (
    [ScanWorkflowScanSegmentID] UNIQUEIDENTIFIER NOT NULL,
    [ScanWorkflowScanID]        UNIQUEIDENTIFIER NOT NULL,
    [ScannerSerialNumber]       NVARCHAR (10)    NULL,
    [RowVersion]                ROWVERSION       NOT NULL,
    PRIMARY KEY CLUSTERED ([ScanWorkflowScanSegmentID] ASC)
);

