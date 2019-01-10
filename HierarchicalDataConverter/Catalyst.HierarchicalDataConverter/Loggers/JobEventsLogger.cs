namespace DataConverter.Loggers
{
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class JobEventsLogger : IJobEventsLogger
    {
        public int NumberOfEntities { get; set; }

        public void JobCompleted(IJobCompletedQueueItem jobCompletedQueueItem)
        {
            Log.Logger.Information($"JobCompleted: Entities: {jobCompletedQueueItem.NumberOfEntities}");
            this.NumberOfEntities = jobCompletedQueueItem.NumberOfEntities;
        }
    }
}
