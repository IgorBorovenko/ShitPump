CREATE TABLE [Source].[DentalDesktopTRIOS.ScanWorkflowScanSegment] (
    [ScanWorkflowScanSegmentID] UNIQUEIDENTIFIER NOT NULL,
    [ScanWorkflowScanID]        UNIQUEIDENTIFIER NOT NULL,
    [ScannerSerialNumber]       NVARCHAR (10)    NULL,
    [RowVersion]                BINARY (8)       NOT NULL,
    [WholeLoadSucceeded]        BIT              DEFAULT ((0)) NOT NULL,
    PRIMARY KEY CLUSTERED ([ScanWorkflowScanSegmentID] ASC)
);

