namespace DataConverter
{
    using System.Runtime.CompilerServices;

    using Fabric.Databus.Config;

    using Newtonsoft.Json;

    public class HierarchicalConfiguration
    {
        public QueryConfig DatabusConfiguration { get; set; }

        public IClientSpecificConfiguration ClientSpecificConfiguration { get; set; }

        public override string ToString()
        {
            return $"DatabusConfiguration: {JsonConvert.SerializeObject(this.DatabusConfiguration)},\nClientSpecificConfiguration ({this.ClientSpecificConfiguration.GetType()}): {this.ClientSpecificConfiguration}";
        }
    }
}