namespace DataConverter.Loggers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Fabric.Databus.Interfaces.Loggers;

    using Serilog;

    public class QuerySqlLogger : IQuerySqlLogger
    {
        public void SqlQueryCompleted(QuerySqlLogEvent querySqlLogEvent)
        {
            var parametersAsString = string.Join(",", querySqlLogEvent.SqlParameters.Select(a => $"{a.Key} = {a.Value}").ToList());
            Log.Logger.Debug($"SqlQueryCompleted: {querySqlLogEvent.Path} {querySqlLogEvent.TableOrView} {querySqlLogEvent.RowCount} {querySqlLogEvent.TimeElapsed:c} {querySqlLogEvent.Sql} {parametersAsString}");
        }

        public void SqlQueryStarted(QuerySqlLogEvent querySqlLogEvent)
        {
            Log.Logger.Debug($"SqlQueryStarted: {querySqlLogEvent.Path} {querySqlLogEvent.TableOrView}");
        }
    }
}
