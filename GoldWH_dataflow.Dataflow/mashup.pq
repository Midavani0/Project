[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Append"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = Fabric.Warehouse([CreateNavigationProperties = false]){[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data]{[warehouseId = "dbaf5791-cb1a-4789-8fcf-6514ad409cae"]}[Data];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_fx_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "TIME_PERIOD", DestinationColumnName = "time_period"], [SourceColumnName = "OBS_VALUE", DestinationColumnName = "obs_value"], [SourceColumnName = "CURRENCY_DENOM", DestinationColumnName = "currency_denom"], [SourceColumnName = "CURRENCY", DestinationColumnName = "currency"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_fx = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "fcce5f35-a431-4dad-8439-99dadb00053b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_fx", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"TIME_PERIOD", Order.Ascending}})
in
  #"Sorted rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_gdp_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "date", DestinationColumnName = "date"], [SourceColumnName = "value", DestinationColumnName = "value"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_gdp = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "fcce5f35-a431-4dad-8439-99dadb00053b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_gdp", ItemKind = "Table"]}[Data],
  #"Sorted rows" = Table.Sort(#"Navigation 2", {{"date", Order.Ascending}})
in
  #"Sorted rows";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_sensor_zones_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sensor_zone_id", DestinationColumnName = "sensor_zone_id"], [SourceColumnName = "number_of_sensors", DestinationColumnName = "number_of_sensors"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared dim_sensor_zones = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "fcce5f35-a431-4dad-8439-99dadb00053b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "dim_sensor_zones", ItemKind = "Table"]}[Data]
in
  #"Navigation 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "nyctaxi_transformed_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "pickup_location_id", DestinationColumnName = "pickup_location"], [SourceColumnName = "dropoff_location_id", DestinationColumnName = "dropoff_location"], [SourceColumnName = "trip_distance", DestinationColumnName = "trip_distance"], [SourceColumnName = "pickup_datetime", DestinationColumnName = "pickup_datetime"], [SourceColumnName = "date_from", DestinationColumnName = "date_from"], [SourceColumnName = "hour_from", DestinationColumnName = "hour_from"], [SourceColumnName = "dropoff_datetime", DestinationColumnName = "dropoff_datetime"], [SourceColumnName = "date_to", DestinationColumnName = "date_to"], [SourceColumnName = "hour_to", DestinationColumnName = "hour_to"], [SourceColumnName = "fare_amount", DestinationColumnName = "fare_amount"], [SourceColumnName = "tip_amount", DestinationColumnName = "tip_amount"], [SourceColumnName = "tolls_amount", DestinationColumnName = "tolls_amount"], [SourceColumnName = "mta_tax", DestinationColumnName = "mta_tax"], [SourceColumnName = "sales_tax", DestinationColumnName = "sales_tax"], [SourceColumnName = "bcf", DestinationColumnName = "bcf"], [SourceColumnName = "congestion_surcharge", DestinationColumnName = "congestion_surcharge"], [SourceColumnName = "improvement_surcharge", DestinationColumnName = "improvement_surcharge"], [SourceColumnName = "extra", DestinationColumnName = "extra"], [SourceColumnName = "driver_pay", DestinationColumnName = "driver_pay"], [SourceColumnName = "total_amount", DestinationColumnName = "total_amount"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared nyctaxi_transformed = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "fcce5f35-a431-4dad-8439-99dadb00053b"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "nyctaxi_transformed", ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"pickup_location_id", Int64.Type}, {"dropoff_location_id", Int64.Type}})
in
  #"Changed column type";
shared nyctaxi_transformed_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = true, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "GoldPhase_WH"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dbo"]}[Data],
  TableNavigation = Navigation_3{[Name = "FactTaxiDaily"]}[Data]
in
  TableNavigation;
shared dim_sensor_zones_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = true, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "GoldPhase_WH"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dbo"]}[Data],
  TableNavigation = Navigation_3{[Name = "DimSensorZones"]}[Data]
in
  TableNavigation;
shared dim_gdp_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = true, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "GoldPhase_WH"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dbo"]}[Data],
  TableNavigation = Navigation_3{[Name = "DimGDP"]}[Data]
in
  TableNavigation;
shared dim_fx_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = true, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "."]}[Data],
  Navigation_2 = Navigation_1{[displayName = "GoldPhase_WH"]}[Data],
  Navigation_3 = Navigation_2{[Schema = "dbo"]}[Data],
  TableNavigation = Navigation_3{[Name = "DimFX"]}[Data]
in
  TableNavigation;
