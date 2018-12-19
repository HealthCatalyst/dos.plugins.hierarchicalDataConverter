namespace DataConverter
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    /// <summary>
    /// Data Bus currently handles the promotion of data to its final destination (Rest API), therefore this is a no op implementation
    /// </summary>
    public class RestApiDestinationSystem : IDestinationSystem
    {
        private readonly IMetadataServiceClient metadataServiceClient;

        public RestApiDestinationSystem(IMetadataServiceClient metadataServiceClient)
        {
            this.metadataServiceClient = metadataServiceClient ?? throw new ArgumentException("metadataServiceClient cannot be null.");
        }

        public async Task AddEntityOptimizationsAsync(EntityExecution entityExecution, Entity entity, DataMart dataMart, CancellationToken cancellationToken)
        {
            // no op
            await Task.CompletedTask;
        }

        public bool CanHandle(Entity entity)
        {
            if (entity == null)
            {
                throw new ArgumentException("Entity cannot be null.");
            }

            Binding[] bindings = this.metadataServiceClient.GetBindingsForEntityAsync(entity.Id).Result;

            if (bindings == null)
            {
                return false;
            }

            return bindings.Any(b => b.BindingType == HierarchicalDataTransformer.NestedBindingTypeName);
        }

        public async Task CreateDestinationEntityAsync(EntityExecution entityExecution, Entity entity, DataMart dataMart, CancellationToken cancellationToken)
        {
            // no op
            await Task.CompletedTask;
        }

        public async Task CreatePhysicalEntityAsync(EntityExecution entityExecution, Entity entity, DataMart dataMart, CancellationToken cancellationToken)
        {
            // no op
            await Task.CompletedTask;
        }

        public Task<IDictionary<string, object>> GetExampleDataAsync(EntityExecution entityExecution, Entity entity, ICollection<Field> fieldsToUpdate, CancellationToken cancellationToken)
        {
            IDictionary<string, object> result = new Dictionary<string, object>();
            return Task.FromResult(result);
        }

        public async Task<long> PromoteStagedDataAsync(EntityExecution entityExecution, Entity entity, DataMart dataMart, CancellationToken cancellationToken)
        {
            // no op: Databus takes care of this
            await Task.CompletedTask;
            return Convert.ToInt64(0);
        }
    }
}
