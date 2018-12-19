namespace Catalyst.PluginTester.DummyImplementations
{
    using System;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    public class TestLoggingRepository : ILoggingRepository
    {
        public void LogPreEvent(
            IExecution execution,
            string overrideClassName = null,
            string overrideMethodName = null,
            string className = null,
            string methodName = null)
        {
        }

        public void LogPostEvent(
            IExecution execution,
            string overrideClassName = null,
            string overrideMethodName = null,
            string className = null,
            string methodName = null)
        {
        }

        public void LogError(
            IExecution execution,
            string errorMessage,
            string overrideClassName = null,
            string overrideMethodName = null,
            string className = null,
            string methodName = null)
        {
        }

        public void LogError(
            IExecution execution,
            Exception exception,
            string overrideClassName = null,
            string overrideMethodName = null,
            string className = null,
            string methodName = null)
        {
            throw new NotImplementedException();
        }

        public void LogWarning(
            IExecution execution,
            string warningMessage,
            string overrideClassName = null,
            string overrideMethodName = null,
            string className = null,
            string methodName = null)
        {
        }

        public void LogInformation(
            IExecution execution,
            string informationMessage,
            string overrideClassName = null,
            string overrideMethodName = null,
            string detailedInformationMessage = null,
            string className = null,
            string methodName = null)
        {
        }

        public void LogExecutionStatement(string className, string methodName, IExecution execution, string statement)
        {
        }

        public void LogFailedExecutionStatement(
            string className,
            string methodName,
            IExecution execution,
            string statement,
            string errorMessage)
        {
        }

        public void FinalizeBatchExecutionLogs(BatchExecution batchExecution)
        {
        }
    }
}
