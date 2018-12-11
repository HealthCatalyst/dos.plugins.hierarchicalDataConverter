namespace Catalyst.PluginTester
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    public class TestMetadataServiceClient : IMetadataServiceClient
    {
        private List<Binding> bindings;

        private List<Entity> entities;

        private List<Field> entityFields;


        public void Init(List<Entity> entities1, List<Binding> bindings1, List<Field> fields)
        {
            this.entities = entities1;
            this.bindings = bindings1;
            this.entityFields = fields;
        }

        public Task<DataMart> GetDataMartAsync(int dataMartId, bool includeFullDataMart = false, string actingUser = null)
        {
            throw new System.NotImplementedException();
        }

        public Task<Entity[]> GetEntitiesForDataMartAsync(int dataMartId)
        {
            throw new System.NotImplementedException();
        }

        public async Task<Binding[]> GetBindingsForDataMartAsync(int dataMartId)
        {
            return await Task.FromResult(this.bindings.ToArray());
        }

        public Task<Binding[]> GetBindingsForEntityAsync(int entityId)
        {
            throw new System.NotImplementedException();
        }

        public Task<Connection> GetConnectionAsync(int connectionId)
        {
            throw new System.NotImplementedException();
        }

        public Task<ObjectAttributeValue> GetSystemAttributeAsync(string attributeName)
        {
            throw new System.NotImplementedException();
        }

        public async Task<Field[]> GetEntityFieldsAsync(Entity entity)
        {
            return await Task.FromResult(this.entityFields.Where(field => field.EntityId == entity.Id).ToArray());
        }

        public async Task<Entity> GetEntityAsync(int entityId)
        {
            return await Task.FromResult(this.entities.First(entity => entity.Id == entityId));
        }

        public Task UpdateEntityAsync(Entity entity)
        {
            throw new System.NotImplementedException();
        }

        public Task<Binding> GetBindingAsync(int bindingId)
        {
            throw new System.NotImplementedException();
        }

        public Task<Resource> GetResourceAsync(int resourceId)
        {
            throw new System.NotImplementedException();
        }

    }
}