namespace DataConverter
{
    public static class AttributeNames
    {
        // Hierarchical Plugin Attributes
        public const string HierarchicalPluginLogLevel = "HierarchicalPlugin.LogLevel";
        
        // Hierarchical Binding Attributes
        public const string Cardinality = "Cardinality";

        public const string ParentKeyFields = "ParentKeyFields";

        public const string ChildKeyFields = "ChildKeyFields";

        public const string GenerationGap = "GenerationGap";

        public const string ConnectionString = "ConnectionString";

        public const string ClientSpecificConfigurationKey = "ClientSpecificConfigurationKey";

        // Databus specific attributes
        public const string MaxEntitiesToLoad = "HierarchicalPlugin.MaxEntitiesToLoad";

        public const string EntitiesPerBatch = "HierarchicalPlugin.EntitiesPerBatch";

        public const string EntitiesPerUploadFile = "HierarchicalPlugin.EntitiesPerUploadFile";

        public const string LocalSaveFolder = "HierarchicalPlugin.LocalSaveFolder";

        public const string WriteTempFilesToDisk = "HierarchicalPlugin.WriteTempFilesToDisk";

        public const string DetailedTempFiles = "HierarchicalPlugin.DetailedTempFiles";

        public const string CompressFiles = "HierarchicalPlugin.CompressFiles";

        public const string UploadToUrl = "HierarchicalPlugin.UploadToUrl";

        // Entity Attributes
        public const string HttpMethod = "HttpMethod";

        public const string Endpoint = "Endpoint";

        // Binding Attributes
        public const string ServiceUrl = "ServiceUrl";
    }
}
