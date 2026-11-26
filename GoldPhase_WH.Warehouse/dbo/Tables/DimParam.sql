CREATE TABLE [dbo].[DimParam] (
    [parameter_id]    INT          NOT NULL,
    [parameter_name]  VARCHAR (50) NOT NULL,
    [parameter_units] VARCHAR (50) NOT NULL
);


GO

ALTER TABLE [dbo].[DimParam]
    ADD PRIMARY KEY NONCLUSTERED ([parameter_id] ASC) NOT ENFORCED;


GO