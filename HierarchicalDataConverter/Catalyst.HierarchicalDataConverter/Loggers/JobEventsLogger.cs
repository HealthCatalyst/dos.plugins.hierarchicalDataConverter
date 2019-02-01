namespace DataConverter.Loggers
{
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class JobEventsLogger : IJobEventsLogger
    {
        private readonly ILoggingRepository loggingRepository;

        private readonly BindingExecution bindingExecution;


        public JobEventsLogger(ILoggingRepository loggingRepository, BindingExecution bindingExecution)
        {
            this.loggingRepository = loggingRepository;
            this.bindingExecution = bindingExecution;
        }

        public int NumberOfEntities { get; set; }

        public void JobCompleted(IJobCompletedQueueItem jobCompletedQueueItem)
        {
            var information = $"JobCompleted: Entities: {jobCompletedQueueItem.NumberOfEntities}";
            this.loggingRepository.LogInformation(this.bindingExecution, information);
            Log.Logger.Information(information);
            this.NumberOfEntities = jobCompletedQueueItem.NumberOfEntities;
        }
    }
}
