[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared NYC_GDP = let
  Source = Json.Document(Web.Contents("https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&limit=100")),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Removed top rows" = Table.Skip(#"Converted to table", 1),
  Navigation = #"Removed top rows"{0}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Ignore),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"date", "value"}, {"date", "value"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Expanded Column1", {{"date", type date}, {"value", type number}})
in
  #"Changed column type";
[BindToDefaultDestination = true]
shared EUR_USD = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, Encoding = 65001]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Removed columns" = Table.RemoveColumns(#"Promoted headers", {"SOURCE_PUB"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Removed columns", {"UNIT_MULT", "UNIT", "TITLE_COMPL", "TITLE", "SOURCE_AGENCY", "NAT_TITLE", "DECIMALS", "COVERAGE", "COMPILATION", "UNIT_INDEX_BASE", "PUBL_PUBLIC", "PUBL_MU", "PUBL_ECB", "DOM_SER_IDS", "DISS_ORG", "COMPILING_ORG", "COLLECTION", "BREAKS", "TIME_FORMAT", "OBS_COM", "OBS_PRE_BREAK", "OBS_CONF", "OBS_STATUS", "EXR_SUFFIX", "EXR_TYPE", "FREQ", "KEY"})
in
  #"Removed columns 1";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "a0e81625-9b2f-4084-b2a7-c27c95a5d6d9"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "dbo", ItemKind = "Schema"]}[Data]
in
  #"Navigation 3";
