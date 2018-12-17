namespace DataConverter
{
    using Fabric.Databus.Config;

    public class HierarchicalConfiguration
    {
        public QueryConfig DatabusConfiguration { get; set; }

        public IClientSpecificConfiguration ClientSpecificConfiguration { get; set; }
    }
}