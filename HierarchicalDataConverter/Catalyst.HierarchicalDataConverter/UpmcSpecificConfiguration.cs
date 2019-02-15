namespace DataConverter
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    using Catalyst.Platform.CommonExtensions;

    using DataConverter.Properties;

    using Newtonsoft.Json;

    public class UpmcSpecificConfiguration : IClientSpecificConfiguration
    {
        public UpmcSpecificConfiguration(IDictionary<string, object> values)
        {
            object name;
            values.TryGetValue(nameof(this.Name), out name);

            object baseUrl;
            values.TryGetValue(nameof(this.BaseUrl), out baseUrl);

            object appId;
            values.TryGetValue(nameof(this.AppId), out appId);

            object appSecret;
            values.TryGetValue(nameof(this.AppSecret), out appSecret);

            object tenantSecret;
            values.TryGetValue(nameof(this.TenantSecret), out tenantSecret);

            if (!(name is string) || !(baseUrl is string) || !(appId is string) || !(appSecret is string) || !(tenantSecret is string))
            {
                throw new InvalidOperationException(Resources.CannotCreateUpmcConfiguration);
            }

            this.Name = (string)name;
            this.BaseUrl = (string)baseUrl;
            this.AppId = (string)appId;
            this.AppSecret = (string)appSecret;
            this.TenantSecret = (string)tenantSecret;
        }

        public string Name { get; set; }

        public string BaseUrl { get; set; }

        public string AppId { get; set; }

        public string AppSecret { get; set; }

        public string TenantSecret { get; set; }

        public override string ToString()
        {
            string serialized = JsonConvert.SerializeObject(this);
            return serialized.Replace(this.TenantSecret, "********").Replace(this.AppSecret, "********");
        }
    }
}
