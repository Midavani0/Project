CREATE TABLE [dbo].[DimFX] (
    [time_period]    DATE       NOT NULL,
    [obs_value]      FLOAT (53) NOT NULL,
    [currency_denom] CHAR (3)   NOT NULL,
    [currency]       CHAR (3)   NOT NULL
)
WITH (CLUSTER BY ([time_period]));


GO

ALTER TABLE [dbo].[DimFX]
    ADD CONSTRAINT [FX_Date] FOREIGN KEY ([time_period]) REFERENCES [dbo].[DimDate] ([date]) NOT ENFORCED;


GO