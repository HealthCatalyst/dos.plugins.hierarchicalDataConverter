namespace Catalyst.PluginTester
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using JetBrains.Annotations;

    public class TestMetadataServiceClient : IMetadataServiceClient
    {
        private DataMart dataMart;

        public void Init(DataMart dataMart1)
         {
             this.dataMart = dataMart1 ?? throw new ArgumentNullException(nameof(dataMart1));
         }

        public Task<DataMart> GetDataMartAsync(int dataMartId, bool includeFullDataMart = false, string actingUser = null)
        {
            throw new NotImplementedException();
        }

        public async Task<Entity[]> GetEntitiesForDataMartAsync(int dataMartId)
        {
            return await Task.FromResult(this.dataMart.Entities.ToArray());
        }

        public async Task<Binding[]> GetBindingsForDataMartAsync(int dataMartId)
        {
            return await Task.FromResult(this.dataMart.Bindings.ToArray());
        }

        public Task<Binding[]> GetBindingsForEntityAsync(int entityId)
        {
            return Task.FromResult(this.dataMart.Bindings.Where(binding => binding.DestinationEntityId == entityId).ToArray());
        }

        public Task<Connection> GetConnectionAsync(int connectionId)
        {
            return Task.FromResult(this.dataMart.Connections.First());
        }

        public Task<ObjectAttributeValue> GetSystemAttributeAsync(string attributeName)
        {
            throw new NotImplementedException();
        }

        public async Task<Field[]> GetEntityFieldsAsync(Entity entity)
        {
            return await Task.FromResult(entity.Fields.ToArray());
        }

        public async Task<Entity> GetEntityAsync(int entityId)
        {
            return await Task.FromResult(this.dataMart.Entities.First(entity => entity.Id == entityId));
        }

        public Task UpdateEntityAsync(Entity entity)
        {
            throw new NotImplementedException();
        }

        public Task<Binding> GetBindingAsync(int bindingId)
        {
            return Task.FromResult(this.dataMart.Bindings.First(binding => binding.Id == bindingId));
        }

        public Task<Resource> GetResourceAsync(int resourceId)
        {
            throw new NotImplementedException();
        }
    }
}