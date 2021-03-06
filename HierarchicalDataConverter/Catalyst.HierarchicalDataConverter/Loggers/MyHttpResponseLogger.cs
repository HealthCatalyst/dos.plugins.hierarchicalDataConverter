﻿namespace DataConverter.Loggers
{
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using Fabric.Shared.ReliableHttp.Interfaces;

    using Serilog;

    public class MyHttpResponseLogger : IHttpResponseLogger
    {
        private readonly ILoggingRepository loggingRepository;

        private readonly BindingExecution bindingExecution;

        public MyHttpResponseLogger(ILoggingRepository loggingRepository, BindingExecution bindingExecution)
        {
            this.loggingRepository = loggingRepository;
            this.bindingExecution = bindingExecution;
        }

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
            var information = $"{responseStatusCode} {httpMethod} {fullUri} {stopwatchElapsedMilliseconds}ms {content}";
            this.loggingRepository.LogInformation(this.bindingExecution, information);
            Log.Logger.Information(information);
        }
    }
}
