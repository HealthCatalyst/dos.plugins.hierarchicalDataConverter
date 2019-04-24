namespace DataConverter
{
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class RowCounterBatchEventsLogger : IBatchEventsLogger
    {
        private readonly ILogger pluginLogger;
        private readonly BindingExecution bindingExecution;

        public RowCounterBatchEventsLogger(ILogger logger, BindingExecution bindingExecution)
        {
            this.pluginLogger = logger;
            this.bindingExecution = bindingExecution;
        }

        public void BatchCompleted(IBatchCompletedQueueItem batchCompletedQueueItem)
        {
            var information = $"BatchCompleted: {batchCompletedQueueItem.BatchNumber}  Uploaded Entities: {batchCompletedQueueItem.NumberOfEntitiesUploaded}. [start:{batchCompletedQueueItem.Start}, end:{batchCompletedQueueItem.End}]";
            LoggingHelper.Info(information, this.bindingExecution);
            this.pluginLogger.Information(information);
        }

        public void BatchStarted(IBatchCompletedQueueItem batchStartedQueueItem)
        {
            var information = $"BatchStarted: {batchStartedQueueItem.BatchNumber} [start:{batchStartedQueueItem.Start}, end:{batchStartedQueueItem.End}]";
            LoggingHelper.Info(information, this.bindingExecution);
            this.pluginLogger.Debug(information);
        }
    }
}
