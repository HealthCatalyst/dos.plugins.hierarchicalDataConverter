namespace DataConverter.Loggers
{
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;

    using Fabric.Shared.ReliableHttp.Interfaces;

    using Serilog;

    public class MyHttpResponseLogger : IHttpResponseLogger
    {
        public async Task LogResponseAsync(
            string requestId,
            HttpMethod httpMethod,
            Uri fullUri,
            Stream requestContent,
            HttpStatusCode responseStatusCode,
            HttpContent responseContent,
            long stopwatchElapsedMilliseconds)
        {
            var content = await responseContent.ReadAsStringAsync();
            Log.Logger.Information($"{responseStatusCode} {httpMethod} {fullUri} {stopwatchElapsedMilliseconds}ms {content}");
        }
    }
}
