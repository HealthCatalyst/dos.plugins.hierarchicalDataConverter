namespace DataConverter.Loggers
{
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Databus.Interfaces.Queues;

    using Serilog;

    public class JobEventsLogger : IJobEventsLogger
    {
        private readonly ILogger pluginLogger;

        private readonly BindingExecution bindingExecution;
        
        public JobEventsLogger(ILogger logger, BindingExecution bindingExecution)
        {
            this.pluginLogger = logger;
            this.bindingExecution = bindingExecution;
        }

        public int NumberOfEntities { get; set; }

        public void JobCompleted(IJobCompletedQueueItem jobCompletedQueueItem)
        {
            var information = $"JobCompleted: Entities: {jobCompletedQueueItem.NumberOfEntities}";
            LoggingHelper.Info(information, this.bindingExecution);
            this.pluginLogger.Information(information);
            this.NumberOfEntities = jobCompletedQueueItem.NumberOfEntities;
        }
    }
}
