CREATE TABLE [dbo].[FactTaxiDaily] (
    [trip_id]               BIGINT        IDENTITY NOT NULL,
    [pickup_location]       INT           NULL,
    [dropoff_location]      INT           NULL,
    [trip_distance]         FLOAT (53)    NULL,
    [pickup_datetime]       DATETIME2 (6) NOT NULL,
    [date_from]             DATE          NOT NULL,
    [hour_from]             SMALLINT      NOT NULL,
    [dropoff_datetime]      DATETIME2 (6) NOT NULL,
    [date_to]               DATE          NOT NULL,
    [hour_to]               SMALLINT      NOT NULL,
    [fare_amount]           FLOAT (53)    NULL,
    [tip_amount]            FLOAT (53)    NULL,
    [tolls_amount]          FLOAT (53)    NULL,
    [airport_fee]           FLOAT (53)    NULL,
    [mta_tax]               FLOAT (53)    NULL,
    [ehail_fee]             FLOAT (53)    NULL,
    [sales_tax]             FLOAT (53)    NULL,
    [bcf]                   FLOAT (53)    NULL,
    [congestion_surcharge]  FLOAT (53)    NULL,
    [improvement_surcharge] FLOAT (53)    NULL,
    [extra]                 FLOAT (53)    NULL,
    [driver_pay]            FLOAT (53)    NULL,
    [total_amount]          FLOAT (53)    NULL
)
WITH (CLUSTER BY ([date_from]));


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [Taxi_DODate] FOREIGN KEY ([date_to]) REFERENCES [dbo].[DimDate] ([date]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [Taxi_DOLocation] FOREIGN KEY ([dropoff_location]) REFERENCES [dbo].[DimTaxiZone,] ([taxi_zone_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [Taxi_PUDate] FOREIGN KEY ([date_from]) REFERENCES [dbo].[DimDate] ([date]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD CONSTRAINT [Taxi_PULocation] FOREIGN KEY ([pickup_location]) REFERENCES [dbo].[DimTaxiZone,] ([taxi_zone_id]) NOT ENFORCED;


GO

ALTER TABLE [dbo].[FactTaxiDaily]
    ADD PRIMARY KEY NONCLUSTERED ([trip_id] ASC) NOT ENFORCED;


GO