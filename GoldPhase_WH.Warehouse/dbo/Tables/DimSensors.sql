CREATE TABLE [dbo].[DimSensors] (
    [sensor_id]             INT        NOT NULL,
    [sensor_zone_id]        INT        NOT NULL,
    [coordinates_longitude] FLOAT (53) NOT NULL,
    [coordinates_latitude]  FLOAT (53) NOT NULL
);


GO

ALTER TABLE [dbo].[DimSensors]
    ADD CONSTRAINT [sensor_sensorZones] FOREIGN KEY ([sensor_zone_id]) REFERENCES [dbo].[DimSensorZones] ([sensor_zone_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[DimSensors]
    ADD PRIMARY KEY NONCLUSTERED ([sensor_id] ASC) NOT ENFORCED;


GO