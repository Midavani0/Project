CREATE TABLE [dbo].[DimSensorZones] (
    [sensor_zone_id]    INT    NOT NULL,
    [number_of_sensors] BIGINT NOT NULL
);


GO

ALTER TABLE [dbo].[DimSensorZones]
    ADD PRIMARY KEY NONCLUSTERED ([sensor_zone_id] ASC) NOT ENFORCED;


GO