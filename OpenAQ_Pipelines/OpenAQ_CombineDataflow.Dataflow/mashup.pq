[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared Parameter1 = "671" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared combine = (Parameter1 as text) => let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "a0e81625-9b2f-4084-b2a7-c27c95a5d6d9"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = Text.Combine({"OpenAQ_Raw.", Parameter1}), ItemKind = "Table"]}[Data],
  #"Changed column type" = Table.TransformColumnTypes(#"Navigation 2", {{"results.summary.min", type number}, {"results.summary.q02", type number}, {"results.summary.q25", type number}, {"results.summary.median", type number}, {"results.summary.q75", type number}, {"results.summary.q98", type number}, {"results.summary.max", type number}, {"results.summary.avg", type number}, {"results.value", type number}})
in
  #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Invoked function_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"Invoked function" = let
  Source = combine(Parameter1)
in
  Source;
shared #"Invoked function_DataDestination" = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "742d1f01-1b60-4625-91e5-0f5292a76adc"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "a0e81625-9b2f-4084-b2a7-c27c95a5d6d9"]}[Data],
  Navigation_3 = Navigation_2{[Id = "OpenAQ_AddedCoordinates", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "OpenAQ_AddedCoordinates_Combined", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
