namespace DataConverter.Loggers
{
    using System;

    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    public class BatchEventsLogger : IBatchEventsLogger
    {
        public void BatchCompleted(IBatchCompletedQueueItem batchCompletedQueueItem)
        {
            Console.WriteLine($"BatchCompleted: {batchCompletedQueueItem.BatchNumber} Start: {batchCompletedQueueItem.Start} End: {batchCompletedQueueItem.End}  Entities: {batchCompletedQueueItem.NumberOfEntities}");
        }

        public void BatchStarted(IBatchCompletedQueueItem batchStartedQueueItem)
        {
            Console.WriteLine($"BatchStarted: {batchStartedQueueItem.BatchNumber} Start: {batchStartedQueueItem.Start} End: {batchStartedQueueItem.End}");
        }
    }
}
