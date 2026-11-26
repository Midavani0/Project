CREATE TABLE [dbo].[FactAirQualityDaily] (
    [measurement_id] BIGINT     IDENTITY NOT NULL,
    [value]          FLOAT (53) NOT NULL,
    [parameter_id]   INT        NOT NULL,
    [sensor_id]      INT        NOT NULL,
    [date]           DATE       NOT NULL,
    [hour_from]      SMALLINT   NOT NULL,
    [hour_to]        SMALLINT   NOT NULL
)
WITH (CLUSTER BY ([date], [parameter_id], [sensor_id]));


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [OpenAQ_Date] FOREIGN KEY ([date]) REFERENCES [dbo].[DimDate] ([date]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [OpenAQ_Param] FOREIGN KEY ([parameter_id]) REFERENCES [dbo].[DimParam] ([parameter_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD CONSTRAINT [OpenAQ_Sensors] FOREIGN KEY ([sensor_id]) REFERENCES [dbo].[DimSensors] ([sensor_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactAirQualityDaily]
    ADD PRIMARY KEY NONCLUSTERED ([measurement_id] ASC) NOT ENFORCED;


GO