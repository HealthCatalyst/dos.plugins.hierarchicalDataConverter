namespace Catalyst.PluginTester.DummyImplementations
{
    using System.Collections.Generic;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Context;

    public class TestProcessingContextWrapper : IProcessingContextWrapper
    {
        public void Dispose()
        {
        }

        public BatchExecution GetBatchExecution(BatchExecution batchExecution)
        {
            throw new System.NotImplementedException();
        }

        public EntityExecution GetEntityExecution(EntityExecution entityExecution)
        {
            throw new System.NotImplementedException();
        }

        public BindingExecution GetBindingExecution(BindingExecution bindingExecution)
        {
            throw new System.NotImplementedException();
        }

        public BatchDefinition GetBatchDefinition(BatchDefinition batchDefinition)
        {
            throw new System.NotImplementedException();
        }

        public Environment GetEnvironment(int? sourceConnectionId, int? destinationConnectionId)
        {
            throw new System.NotImplementedException();
        }

        public ICollection<ValidationRecord> GetValidationRecords(EntityExecution entityExecution)
        {
            throw new System.NotImplementedException();
        }

        public ICollection<FileRecord> GetFileRecords(Binding binding)
        {
            throw new System.NotImplementedException();
        }

        public void AddFileRecord(FileDefinition file, BindingExecution bindingExecution)
        {
            throw new System.NotImplementedException();
        }

        public IReadOnlyCollection<IncrementalQueryToken> GetIncrementalQueryTokens()
        {
            throw new System.NotImplementedException();
        }

        public IncrementalValue GetIncrementalValue(IncrementalConfiguration incrementalConfiguration)
        {
            throw new System.NotImplementedException();
        }

        public SystemAttribute GetSystemAttribute(string attributeName)
        {
            throw new System.NotImplementedException();
        }

        public ExecutionEnvironment GetExecutionEnvironmentByIdOrDefault(int? environmentId, string type)
        {
            throw new System.NotImplementedException();
        }

        public ExecutionEnvironment GetExecutionEnvironment(int environmentId)
        {
            throw new System.NotImplementedException();
        }

        public ExecutionEnvironment GetDefaultExecutionEnvironmentForType(string type)
        {
            throw new System.NotImplementedException();
        }

        public void AddOrUpdateIncrementalValue(
            IncrementalValue maxIncrementalValue,
            IncrementalConfiguration incrementalConfiguration)
        {
            throw new System.NotImplementedException();
        }

        public bool GetFeatureToggleState(string featureToggleName)
        {
            throw new System.NotImplementedException();
        }

        public int SaveChanges()
        {
            throw new System.NotImplementedException();
        }
    }
}