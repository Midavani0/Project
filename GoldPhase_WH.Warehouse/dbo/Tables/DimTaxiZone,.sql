CREATE TABLE [dbo].[DimTaxiZone,] (
    [taxi_zone_id]   INT          NOT NULL,
    [zone]           VARCHAR (50) NOT NULL,
    [service_zone]   VARCHAR (50) NOT NULL,
    [Borough]        VARCHAR (50) NOT NULL,
    [sensor_zone_id] INT          NULL
);


GO

ALTER TABLE [dbo].[DimTaxiZone,]
    ADD CONSTRAINT [TaxiZones_SensorZones] FOREIGN KEY ([sensor_zone_id]) REFERENCES [dbo].[DimSensorZones] ([sensor_zone_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[DimTaxiZone,]
    ADD PRIMARY KEY NONCLUSTERED ([taxi_zone_id] ASC) NOT ENFORCED;


GO