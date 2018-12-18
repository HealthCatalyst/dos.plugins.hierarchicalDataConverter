namespace DataConverter
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;

    public class ChildBindingNoOpDataTransformer : IDataTransformer
    {
        public async Task<long> TransformDataAsync(BindingExecution bindingExecution, Binding binding, Entity entity, CancellationToken cancellationToken)
        {
            return Convert.ToInt64(0);
        }

        public bool CanHandle(BindingExecution bindingExecution, Binding binding, Entity destinationEntity)
        {
            return binding.BindingType == HierarchicalDataTransformer.NestedBindingTypeName;
        }
    }
}
