CREATE TABLE [dbo].[DimDate] (
    [date]         DATE NOT NULL,
    [year]         INT  NOT NULL,
    [quarter]      INT  NOT NULL,
    [day_of_year]  INT  NOT NULL,
    [month]        INT  NOT NULL,
    [day_of_month] INT  NOT NULL,
    [day_of_week]  INT  NOT NULL,
    [week_of_year] INT  NOT NULL
)
WITH (CLUSTER BY ([year]));


GO

ALTER TABLE [dbo].[DimDate]
    ADD PRIMARY KEY NONCLUSTERED ([date] ASC) NOT ENFORCED;


GO