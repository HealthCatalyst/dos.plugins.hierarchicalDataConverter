namespace DataConverter
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;

    /// <summary>
    /// All Child (non-root) "Nested" type bindings are handled as part of the HierarchicalDataTransformer, even though that transformer only picks up the root.
    /// </summary>
    public class ChildBindingNoOpDataTransformer : IDataTransformer
    {
        public async Task<long> TransformDataAsync(BindingExecution bindingExecution, Binding binding, Entity entity, CancellationToken cancellationToken)
        {
            return await Task.FromResult(Convert.ToInt64(0));
        }

        public bool CanHandle(BindingExecution bindingExecution, Binding binding, Entity destinationEntity)
        {
            if (binding == null)
            {
                throw new ArgumentException("Binding cannot be null.");
            }

            return binding.BindingType == HierarchicalDataTransformer.NestedBindingTypeName;
        }
    }
}
