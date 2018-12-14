namespace DataConverter
{
    using System;
    using System.IO;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    public class LoggingHelper2
    {
        private static object threadlock;

        static LoggingHelper2()
        {
            threadlock = new object();
        }

        public static void Debug(string message)
        {
            try
            {
                lock (threadlock)
                {
                    // LoggingHelper.Debug(message); // This is no longer working for some reason
                    // TODO - create plugin specific log
                    File.AppendAllText(
                        $@"C:\Program Files\Health Catalyst\Data-Processing Engine\logs\DataProcessingEngine_PluginOnly.log",
                        $"{DateTime.Now} - HierarchicalDataTransformer: {message}\n\n");

                }
            }
            catch
            {
                //barf
            }
        }
    }
}
