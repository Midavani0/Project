CREATE TABLE [dbo].[DimGDP] (
    [date]  DATE       NOT NULL,
    [value] FLOAT (53) NOT NULL
)
WITH (CLUSTER BY ([date]));


GO

ALTER TABLE [dbo].[DimGDP]
    ADD CONSTRAINT [GDP_Date] FOREIGN KEY ([date]) REFERENCES [dbo].[DimDate] ([date]) NOT ENFORCED;


GO