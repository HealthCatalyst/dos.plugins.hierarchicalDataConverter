namespace DataConverter
{
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class RowCounterBatchEventsLogger : IBatchEventsLogger
    {
        public void BatchCompleted(IBatchCompletedQueueItem batchCompletedQueueItem)
        {
            Log.Logger.Debug($"BatchCompleted: {batchCompletedQueueItem.BatchNumber}  Uploaded Entities: {batchCompletedQueueItem.NumberOfEntitiesUploaded}. [start:{batchCompletedQueueItem.Start}, end:{batchCompletedQueueItem.End}]");
        }

        public void BatchStarted(IBatchCompletedQueueItem batchStartedQueueItem)
        {
            Log.Logger.Debug($"BatchStarted: {batchStartedQueueItem.BatchNumber} [start:{batchStartedQueueItem.Start}, end:{batchStartedQueueItem.End}]");
        }
    }
}
