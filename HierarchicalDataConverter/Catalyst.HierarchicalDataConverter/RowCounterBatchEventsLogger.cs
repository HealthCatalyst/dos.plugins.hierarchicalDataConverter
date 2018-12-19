namespace DataConverter
{
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class RowCounterBatchEventsLogger : IBatchEventsLogger
    {
        public RowCounterBatchEventsLogger()
        {
            this.TotalCount = 0;
        }

        public long TotalCount { get; private set; }

        public void BatchCompleted(IBatchCompletedQueueItem batchCompletedQueueItem)
        {
            Log.Logger.Debug($"Uploaded {batchCompletedQueueItem.NumberOfEntitiesUploaded} of {batchCompletedQueueItem.NumberOfEntities} items. [start:{batchCompletedQueueItem.Start}, end:{batchCompletedQueueItem.End}]");
            this.TotalCount += batchCompletedQueueItem.NumberOfEntitiesUploaded;
        }
    }
}
