namespace DataConverter.Loggers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Fabric.Databus.Interfaces.Loggers;

    public class QuerySqlLogger : IQuerySqlLogger
    {
        public void SqlQueryCompleted(QuerySqlLogEvent querySqlLogEvent)
        {
            var parametersAsString = string.Join(",", querySqlLogEvent.SqlParameters.Select(a => $"{a.Key} = {a.Value}").ToList());
            Console.WriteLine($"SqlQueryCompleted: {querySqlLogEvent.Path} {querySqlLogEvent.TableOrView} {querySqlLogEvent.RowCount} {querySqlLogEvent.TimeElapsed:c} {querySqlLogEvent.Sql} {parametersAsString}");
        }

        public void SqlQueryStarted(QuerySqlLogEvent querySqlLogEvent)
        {
            Console.WriteLine($"SqlQueryStarted: {querySqlLogEvent.Path} {querySqlLogEvent.TableOrView}");
        }
    }
}
