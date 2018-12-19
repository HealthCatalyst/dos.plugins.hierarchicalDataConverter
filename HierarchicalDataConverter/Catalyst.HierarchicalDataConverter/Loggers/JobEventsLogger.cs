namespace DataConverter.Loggers
{
    using System;

    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    public class JobEventsLogger : IJobEventsLogger
    {
        public int NumberOfEntities { get; set; }

        public void JobCompleted(IJobCompletedQueueItem jobCompletedQueueItem)
        {
            Console.WriteLine($"JobCompleted: Entities: {jobCompletedQueueItem.NumberOfEntities}");
            this.NumberOfEntities = jobCompletedQueueItem.NumberOfEntities;
        }
    }
}
