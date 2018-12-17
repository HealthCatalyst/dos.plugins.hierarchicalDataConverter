namespace DataConverter
{
    using System;

    public class UpmcSpecificConfig : IClientSpecificConfiguration
    {
        public string Name { get; set; }

        public string BaseUrl { get; set; }

        public string AppId { get; set; }

        public string AppSecret { get; set; }

        public string TenantId { get; set; }

        public string TenantSecret { get; set; }
    }
}
