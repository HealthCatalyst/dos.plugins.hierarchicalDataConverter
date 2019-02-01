namespace DataConverter
{
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class RowCounterBatchEventsLogger : IBatchEventsLogger
    {
        private readonly ILoggingRepository loggingRepository;

        private readonly BindingExecution bindingExecution;

        public RowCounterBatchEventsLogger(ILoggingRepository loggingRepository, BindingExecution bindingExecution)
        {
            this.loggingRepository = loggingRepository;
            this.bindingExecution = bindingExecution;
        }

        public void BatchCompleted(IBatchCompletedQueueItem batchCompletedQueueItem)
        {
            var information = $"BatchCompleted: {batchCompletedQueueItem.BatchNumber}  Uploaded Entities: {batchCompletedQueueItem.NumberOfEntitiesUploaded}. [start:{batchCompletedQueueItem.Start}, end:{batchCompletedQueueItem.End}]";
            this.loggingRepository.LogInformation(this.bindingExecution, information);
            Log.Logger.Information(information);
        }

        public void BatchStarted(IBatchCompletedQueueItem batchStartedQueueItem)
        {
            var information = $"BatchStarted: {batchStartedQueueItem.BatchNumber} [start:{batchStartedQueueItem.Start}, end:{batchStartedQueueItem.End}]";
            this.loggingRepository.LogInformation(this.bindingExecution, information);
            Log.Logger.Debug(information);
        }
    }
}
