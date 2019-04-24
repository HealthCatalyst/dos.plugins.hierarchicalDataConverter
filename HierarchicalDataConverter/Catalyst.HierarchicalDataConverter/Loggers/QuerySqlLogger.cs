namespace DataConverter.Loggers
{
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Databus.Interfaces.Loggers;

    using Serilog;

    public class QuerySqlLogger : IQuerySqlLogger
    {
        private readonly ILogger pluginLogger;

        private readonly BindingExecution bindingExecution;

        public QuerySqlLogger(ILogger logger, BindingExecution bindingExecution)
        {
            this.pluginLogger = logger;
            this.bindingExecution = bindingExecution;
        }

        public void SqlQueryCompleted(QuerySqlLogEvent querySqlLogEvent)
        {
            var parametersAsString = string.Join(",", querySqlLogEvent.SqlParameters.Select(a => $"{a.Key} = {a.Value}").ToList());
            var information = $"SqlQueryCompleted: {querySqlLogEvent.Path} {querySqlLogEvent.TableOrView} {querySqlLogEvent.RowCount} {querySqlLogEvent.TimeElapsed:c} {querySqlLogEvent.Sql} {parametersAsString}";
            LoggingHelper.Info(information, this.bindingExecution);
            this.pluginLogger.Debug(information);
        }

        public void SqlQueryStarted(QuerySqlLogEvent querySqlLogEvent)
        {
            var information = $"SqlQueryStarted: {querySqlLogEvent.Path} {querySqlLogEvent.TableOrView}";
            LoggingHelper.Info(information, this.bindingExecution);
            this.pluginLogger.Debug(information);
        }
    }
}
