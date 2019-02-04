namespace Catalyst.HierarchicalDataConverter.AutomatedTests
{
    using System;
    using System.IO;
    using System.Net.Http;
    using System.Reflection;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Context;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using DataConverter;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using Moq;

    [TestClass]
    public class ReadConfigurationTests
    {
        private PrivateObject privateMethodRunner;

        [TestInitialize]
        public void Setup()
        {
            // Copy config file into Plugins folder
            string configName = "config.json";
            string directoryName = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            if (directoryName == null)
            {
                Assert.Fail("Could not load config.json to execute tests");
            }

            if (!Directory.Exists(Path.Combine(directoryName, "Plugins")))
            {
                Directory.CreateDirectory(Path.Combine(directoryName, "Plugins"));
            }

            if (!Directory.Exists(Path.Combine(directoryName, "Plugins", "HierarchicalDataConverter")))
            {
                Directory.CreateDirectory(Path.Combine(directoryName, "Plugins", "HierarchicalDataConverter"));
            }

            string configFileLocation = Path.Combine(directoryName, configName);
            string configFileNewLocation = Path.Combine(directoryName, "Plugins", "HierarchicalDataConverter", configName);
            File.Copy(configFileLocation, configFileNewLocation, true);

            var serviceClientMock = new Mock<IMetadataServiceClient>();
            var processingContextWrapperFactoryMock = new Mock<IProcessingContextWrapperFactory>();
            var loggingRepositoryMock = new Mock<ILoggingRepository>();
            var converter = new HierarchicalDataTransformer(serviceClientMock.Object, processingContextWrapperFactoryMock.Object, loggingRepositoryMock.Object);
            this.privateMethodRunner = new PrivateObject(converter);
        }

        [TestMethod]
        public void ReadFullConfiguration()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
            {
                Connection = new Connection
                {
                    AttributeValues =
                                                                     {
                                                                         new ObjectAttributeValue
                                                                             {
                                                                                 AttributeName =
                                                                                     AttributeNames
                                                                                         .ClientSpecificConfigurationKey,
                                                                                 AttributeValue = "UPMC.PHI"
                                                                             }
                                                                     }
                }
            };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("server=localhost;initial catalog=SAM;Trusted_Connection=True;", config.DatabusConfiguration.ConnectionString, "Connection string was not equal.");
            Assert.AreEqual("http://localhost/MyService/MyEndpoint", config.DatabusConfiguration.Url, "Service endpoint was not equal.");
            Assert.AreEqual(100, config.DatabusConfiguration.MaximumEntitiesToLoad, "MaximumEntitiesToLoad was not equal");
            Assert.AreEqual(10, config.DatabusConfiguration.EntitiesPerBatch, "EntitiesPerBatch was not equal");
            Assert.AreEqual(100, config.DatabusConfiguration.EntitiesPerUploadFile, "EntitiesPerUploadFile was not equal");
            Assert.AreEqual($"C:\\Catalyst\\databus\\{binding.Name}_{bindingExecution.BindingId}_{bindingExecution.Id}", config.DatabusConfiguration.LocalSaveFolder, "LocalSaveFolder was not equal");
            Assert.AreEqual(true, config.DatabusConfiguration.WriteTemporaryFilesToDisk, "WriteTemporaryFilesToDisk was not equal");
            Assert.AreEqual(true, config.DatabusConfiguration.WriteDetailedTemporaryFilesToDisk, "WriteDetailedTemporaryFilesToDisk was not equal");
            Assert.AreEqual(false, config.DatabusConfiguration.CompressFiles, "CompressFiles was not equal");
            Assert.AreEqual(false, config.DatabusConfiguration.UploadToUrl, "UploadToUrl was not equal");
            Assert.AreEqual(HttpMethod.Post, config.DatabusConfiguration.UrlMethod, "UrlMethod was not equal");

            Assert.IsTrue(config.ClientSpecificConfiguration is UpmcSpecificConfiguration, "ClientSpecificConfiguration was not of type UpmcSpecificConfiguration.");
            var upmcConfig = (UpmcSpecificConfiguration)config.ClientSpecificConfiguration;
            Assert.AreEqual("phi_name", upmcConfig.Name, "Name was not equal");
            Assert.AreEqual("phi_base", upmcConfig.BaseUrl, "BaseUrl was not equal");
            Assert.AreEqual("phi_app_id", upmcConfig.AppId, "AppId was not equal");
            Assert.AreEqual("phi_app_sec", upmcConfig.AppSecret, "AppSecret was not equal");
            Assert.AreEqual("phi_tenant_sec", upmcConfig.TenantSecret, "TenantSecret was not equal");
        }

        [TestMethod]
        public void ReadConfigurationWithoutClientSpecificConfiguration()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() }; // No ClientSpecificConfiguration attribute
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsNull(config.ClientSpecificConfiguration, "ClientSpecificConfiguration should have been null");
        }

        [TestMethod]
        public void EmptyClientSpecificConfigurationKeyResultsInNoExtraConfiguration()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
                                        {
                                            Connection = new Connection
                                                             {
                                                                 AttributeValues =
                                                                     {
                                                                         new ObjectAttributeValue
                                                                             {
                                                                                 AttributeName =
                                                                                     AttributeNames
                                                                                         .ClientSpecificConfigurationKey,
                                                                                 AttributeValue = string.Empty
                                                                             }
                                                                     }
                                                             }
                                        }; // No ClientSpecificConfiguration attribute
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsNull(config.ClientSpecificConfiguration, "ClientSpecificConfiguration should have been null");
        }

        [TestMethod]
        public void ReadUpmcPhiSpecificValues()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
            {
                Connection = new Connection
                {
                    AttributeValues =
                                                                     {
                                                                         new ObjectAttributeValue
                                                                             {
                                                                                 AttributeName =
                                                                                     AttributeNames
                                                                                         .ClientSpecificConfigurationKey,
                                                                                 AttributeValue = "UPMC.PHI"
                                                                             }
                                                                     }
                }
            };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsTrue(config.ClientSpecificConfiguration is UpmcSpecificConfiguration, "ClientSpecificConfiguration was not of type UpmcSpecificConfiguration.");
            var upmcConfig = (UpmcSpecificConfiguration)config.ClientSpecificConfiguration;
            Assert.AreEqual("phi_name", upmcConfig.Name, "Name was not equal");
            Assert.AreEqual("phi_base", upmcConfig.BaseUrl, "BaseUrl was not equal");
            Assert.AreEqual("phi_app_id", upmcConfig.AppId, "AppId was not equal");
            Assert.AreEqual("phi_app_sec", upmcConfig.AppSecret, "AppSecret was not equal");
            Assert.AreEqual("phi_tenant_sec", upmcConfig.TenantSecret, "TenantSecret was not equal");
        }

        [TestMethod]
        public void ReadUpmcNonPhiSpecificValues()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
            {
                Connection = new Connection
                {
                    AttributeValues =
                                                                     {
                                                                         new ObjectAttributeValue
                                                                             {
                                                                                 AttributeName =
                                                                                     AttributeNames
                                                                                         .ClientSpecificConfigurationKey,
                                                                                 AttributeValue = "UPMC.NonPHI"
                                                                             }
                                                                     }
                }
            };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsTrue(config.ClientSpecificConfiguration is UpmcSpecificConfiguration, "ClientSpecificConfiguration was not of type UpmcSpecificConfiguration.");
            var upmcConfig = (UpmcSpecificConfiguration)config.ClientSpecificConfiguration;
            Assert.AreEqual("non_phi_name", upmcConfig.Name, "Name was not equal");
            Assert.AreEqual("non_phi_base", upmcConfig.BaseUrl, "BaseUrl was not equal");
            Assert.AreEqual("non_phi_app_id", upmcConfig.AppId, "AppId was not equal");
            Assert.AreEqual("non_phi_app_sec", upmcConfig.AppSecret, "AppSecret was not equal");
            Assert.AreEqual("non_phi_tenant_sec", upmcConfig.TenantSecret, "TenantSecret was not equal");
        }

        [TestMethod]
        public void InvalidClientSpecificConfigurationKey()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
            {
                Connection = new Connection
                {
                    AttributeValues =
                                                                     {
                                                                         new ObjectAttributeValue
                                                                             {
                                                                                 AttributeName =
                                                                                     AttributeNames
                                                                                         .ClientSpecificConfigurationKey,
                                                                                 AttributeValue = "UPMC.Garbage"
                                                                             }
                                                                     }
                }
            };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var ex = Assert.ThrowsException<TargetInvocationException>(() => this.privateMethodRunner.Invoke("GetConfiguration", args));
            Assert.IsInstanceOfType(ex.InnerException, typeof(InvalidOperationException));
        }

        [TestMethod]
        public void ReadConnectionStringFromSourceConnectionString()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection
                                                         {
                                                             AttributeValues =
                                                                 {
                                                                     new ObjectAttributeValue
                                                                         {
                                                                             AttributeName = AttributeNames.ConnectionString,
                                                                             AttributeValue =
                                                                                 "someConnectionString;with-stuff;in-it"
                                                                         }
                                                                 }
                                                         }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("someConnectionString;with-stuff;in-it", config.DatabusConfiguration.ConnectionString, "Connection string was not equal.");
        }

        [TestMethod]
        public void BuildConnectionStringFromSourceConnectionServerAndDatabase()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("server=localhost;initial catalog=SAM;Trusted_Connection=True;", config.DatabusConfiguration.ConnectionString, "Connection string was not equal.");
        }

        [TestMethod]
        public void MissingConnectionStringThrowsException()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection() };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var ex = Assert.ThrowsException<TargetInvocationException>(() => this.privateMethodRunner.Invoke("GetConfiguration", args));
            Assert.IsInstanceOfType(ex.InnerException, typeof(ArgumentException));
        }

        [TestMethod]
        public void UrlFromEntityAndConnectionAttributes()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
                                        {
                                            Connection =
                                                new Connection
                                                    {
                                                        AttributeValues =
                                                            {
                                                                new ObjectAttributeValue
                                                                    {
                                                                        AttributeName = AttributeNames.ServiceUrl,
                                                                        AttributeValue = "https://some-server/SomeService"
                                                                    }
                                                            }
                                                    },
                                            AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.Endpoint, AttributeValue = "SomeEndpoint" } }
                                        };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("https://some-server/SomeService/SomeEndpoint", config.DatabusConfiguration.Url, "Url was not equal");
        }

        [TestMethod]
        public void DefaultUrlFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("http://localhost/MyService/MyEndpoint", config.DatabusConfiguration.Url, "Url was not equal");
        }

        [TestMethod]
        public void MaxEntitiesFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.MaxEntitiesToLoad, AttributeValue = "31" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(31, config.DatabusConfiguration.MaximumEntitiesToLoad, "MaximumEntitiesToLoad was not equal");
        }

        [TestMethod]
        public void DefaultMaxEntitiesFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(100, config.DatabusConfiguration.MaximumEntitiesToLoad, "MaximumEntitiesToLoad was not equal");
        }

        [TestMethod]
        public void EntitiesPerBatchFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.EntitiesPerBatch, AttributeValue = "31" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(31, config.DatabusConfiguration.EntitiesPerBatch, "EntitiesPerBatch was not equal");
        }

        [TestMethod]
        public void DefaultEntitiesPerBatchFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(10, config.DatabusConfiguration.EntitiesPerBatch, "EntitiesPerBatch was not equal");
        }

        [TestMethod]
        public void EntitiesPerUploadFileFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.EntitiesPerUploadFile, AttributeValue = "31" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(31, config.DatabusConfiguration.EntitiesPerUploadFile, "EntitiesPerUploadFile was not equal");
        }

        [TestMethod]
        public void DefaultEntitiesPerUploadFileFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(100, config.DatabusConfiguration.EntitiesPerUploadFile, "EntitiesPerUploadFile was not equal");
        }

        [TestMethod]
        public void LocalSaveFolderFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.LocalSaveFolder, AttributeValue = "D:\\MyDirectory" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("D:\\MyDirectory\\MyNestedBinding_1_2", config.DatabusConfiguration.LocalSaveFolder, "LocalSaveFolder was not equal");
        }

        [TestMethod]
        public void DefaultLocalSaveFolderFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual("C:\\Catalyst\\databus\\MyNestedBinding_1_2", config.DatabusConfiguration.LocalSaveFolder, "LocalSaveFolder was not equal");
        }

        [TestMethod]
        public void WriteTemporaryFilesToDiskFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.WriteTempFilesToDisk, AttributeValue = "false" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsFalse(config.DatabusConfiguration.WriteTemporaryFilesToDisk, "WriteTemporaryFilesToDisk was not false");
        }

        [TestMethod]
        public void DefaultWriteTemporaryFilesToDiskFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsTrue(config.DatabusConfiguration.WriteTemporaryFilesToDisk, "WriteTemporaryFilesToDisk was not true");
        }

        [TestMethod]
        public void DetailedTempFilesFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.DetailedTempFiles, AttributeValue = "false" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsFalse(config.DatabusConfiguration.WriteDetailedTemporaryFilesToDisk, "WriteDetailedTemporaryFilesToDisk was not false");
        }

        [TestMethod]
        public void DefaultDetailedTempFilesFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsTrue(config.DatabusConfiguration.WriteDetailedTemporaryFilesToDisk, "WriteDetailedTemporaryFilesToDisk was not true");
        }

        [TestMethod]
        public void CompressFilesFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.CompressFiles, AttributeValue = "true" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsTrue(config.DatabusConfiguration.CompressFiles, "CompressFiles was not true");
        }

        [TestMethod]
        public void DefaultCompressFilesFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsFalse(config.DatabusConfiguration.CompressFiles, "CompressFiles was not false");
        }

        [TestMethod]
        public void UploadToUrlFromBindingAttribute()
        {
            // Arrange
            var binding = new Binding
                              {
                                  Name = "MyNestedBinding",
                                  SourceConnection = new Connection { Server = "localhost", Database = "SAM" },
                                  AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.UploadToUrl, AttributeValue = "true" } }
                              };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsTrue(config.DatabusConfiguration.UploadToUrl, "UploadToUrl was not true");
        }

        [TestMethod]
        public void DefaultUploadToUrlFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.IsFalse(config.DatabusConfiguration.UploadToUrl, "UploadToUrl was not false");
        }

        [TestMethod]
        public void HttpVerbFromEntityAttribute()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
                                        {
                                            Connection = new Connection(),
                                            AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.HttpMethod, AttributeValue = "Put" } }
                                        };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(HttpMethod.Put, config.DatabusConfiguration.UrlMethod, "UrlMethod was not equal");
        }

        [TestMethod]
        public void DefaultHttpVerbFromConfigFile()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity { Connection = new Connection() };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var config = (HierarchicalConfiguration)this.privateMethodRunner.Invoke("GetConfiguration", args);

            // Assert
            Assert.AreEqual(HttpMethod.Post, config.DatabusConfiguration.UrlMethod, "UrlMethod was not equal");
        }

        [TestMethod]
        public void UnsupportedHttpVerbThrowsException()
        {
            // Arrange
            var binding = new Binding { Name = "MyNestedBinding", SourceConnection = new Connection { Server = "localhost", Database = "SAM" } };
            var bindingExecution = new BindingExecution { BindingId = 1, Id = 2 };
            var destinationEntity = new Entity
                                        {
                                            Connection = new Connection(),
                                            AttributeValues = { new ObjectAttributeValue { AttributeName = AttributeNames.HttpMethod, AttributeValue = "Delete" } }
                                        };
            var args = new object[] { binding, bindingExecution, destinationEntity };

            // Act
            var ex = Assert.ThrowsException<TargetInvocationException>(() => this.privateMethodRunner.Invoke("GetConfiguration", args));
            Assert.IsInstanceOfType(ex.InnerException, typeof(ArgumentException));
        }
    }
}
