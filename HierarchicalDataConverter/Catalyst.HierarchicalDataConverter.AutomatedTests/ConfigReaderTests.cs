using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Catalyst.HierarchicalDataConverter.AutomatedTests
{
    using System.IO;
    using System.Reflection;

    using Castle.Core.Logging;

    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Context;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using DataConverter;

    using Fabric.Databus.Config;

    using log4net;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using Moq;

    [TestClass]
    public class ConfigReaderTests
    {
        [TestMethod]
        public void TestReadingTheConfig()
        {
            //Copy config file into Plugins folder
            string configName = "config.json";
            string directoryName = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            if (!Directory.Exists(Path.Combine(directoryName, "Plugins")))
            {
                Directory.CreateDirectory(Path.Combine(directoryName, "Plugins"));
            }
            if (!Directory.Exists(Path.Combine(directoryName, "Plugins", "HierarchicalDataConverter")))
            {
                Directory.CreateDirectory(Path.Combine(directoryName, "Plugins", "HierarchicalDataConverter"));
            }
            
            string configFileLocation = Path.Combine(directoryName, configName);
            string configFileNewLocation = Path.Combine(
                directoryName,
                "Plugins",
                "HierarchicalDataConverter",
                configName);
            File.Copy(configFileLocation, configFileNewLocation, true);
            var serviceClientMock = new Mock<IMetadataServiceClient>();
            var processingContextWrapperFactoryMock = new Mock<IProcessingContextWrapperFactory>();
            var loggingRepositoryMock = new Mock<ILoggingRepository>();
            var converter = new HierarchicalDataTransformer(serviceClientMock.Object, processingContextWrapperFactoryMock.Object, loggingRepositoryMock.Object);
            var privateMethodRunner = new PrivateObject(converter);
            var args = new object[1] { configName };
            var config = (HierarchicalConfiguration)privateMethodRunner.Invoke("GetConfigurationFromJsonFile", args);

            Assert.AreEqual("server=localhost;initial catalog=EDWAdmin;Trusted_Connection=True;", config.DatabusConfiguration.ConnectionString);
            Assert.AreEqual("https://hc2342.hqcatalyst.local/DataProcessingService/v1/BatchExecutions", config.DatabusConfiguration.Url);
            Assert.AreEqual(100, config.DatabusConfiguration.MaximumEntitiesToLoad);
            Assert.AreEqual(10, config.DatabusConfiguration.EntitiesPerBatch);
            Assert.AreEqual(100, config.DatabusConfiguration.EntitiesPerUploadFile);
            Assert.AreEqual("C:\\Catalyst\\databus", config.DatabusConfiguration.LocalSaveFolder);
            Assert.AreEqual(true, config.DatabusConfiguration.WriteTemporaryFilesToDisk);
            Assert.AreEqual(true, config.DatabusConfiguration.WriteDetailedTemporaryFilesToDisk);
            Assert.AreEqual(false, config.DatabusConfiguration.CompressFiles);
            Assert.AreEqual(false, config.DatabusConfiguration.UploadToUrl);

            var upmcConfig = (UpmcSpecificConfig)config.ClientSpecificConfiguration;
            Assert.AreEqual("REDACTED_Name", upmcConfig.Name);
            Assert.AreEqual("REDACTED_BaseUrl", upmcConfig.BaseUrl);
            Assert.AreEqual("REDACTED_AppId", upmcConfig.AppId);
            Assert.AreEqual("REDACTED_AppSecret", upmcConfig.AppSecret);
            Assert.AreEqual("REDACTED_TenantId", upmcConfig.TenantId);
            Assert.AreEqual("REDACTED_TenantSecret", upmcConfig.TenantSecret);
        }
    }
}
