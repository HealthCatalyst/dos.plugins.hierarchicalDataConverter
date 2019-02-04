// --------------------------------------------------------------------------------------------------------------------
// <copyright file="HierarchicalDataTransformer.cs" company="Health Catalyst">
//   Copyright 2018 by Health Catalyst.  All rights reserved.
// </copyright>
// <summary>
//   The hierarchical data transformer.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace DataConverter
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net.Http;
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Context;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;
    using Catalyst.Platform.CommonExtensions;

    using DataConverter.Loggers;
    using DataConverter.Properties;

    using Fabric.Databus.Client;
    using Fabric.Databus.Config;
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Shared.ReliableHttp.Interfaces;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using Serilog;
    using Serilog.Core;
    using Serilog.Events;

    using Unity;

    /// <summary>
    /// The hierarchical data transformer.
    /// </summary>
    public class HierarchicalDataTransformer : IDataTransformer
    {
        public const string NestedBindingTypeName = "Nested";
        private const string SourceEntitySourceColumnSeparator = "__";

        private static LoggingLevelSwitch levelSwitch = new LoggingLevelSwitch();

        private readonly IMetadataServiceClient metadataServiceClient;
        private readonly IProcessingContextWrapperFactory processingContextWrapperFactory;
        private readonly ILoggingRepository loggingRepository;

        private readonly DatabusRunner runner;

        /// <summary>
        /// Setup Logger
        /// </summary>
        static HierarchicalDataTransformer()
        {
            SetupSerilogLogger();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformer"/> class.
        /// </summary>
        /// <param name="metadataServiceClient"></param>
        /// <param name="processingContextWrapperFactory"></param>
        /// <param name="loggingRepository"></param>
        public HierarchicalDataTransformer(IMetadataServiceClient metadataServiceClient, IProcessingContextWrapperFactory processingContextWrapperFactory, ILoggingRepository loggingRepository)
        {
            this.metadataServiceClient = metadataServiceClient ?? throw new ArgumentException("metadataServiceClient cannot be null.");
            this.processingContextWrapperFactory = processingContextWrapperFactory ?? throw new ArgumentException("ProcessingContextWrapperFactory cannot be null.");
            this.loggingRepository = loggingRepository ?? throw new ArgumentException("log4NetLogger cannot be null.");

            this.runner = new DatabusRunner();

            this.LogDebug("Successfully created HierarchicalDataTransformer instance.");
        }

        /// <summary>
        /// Transform the data from SQL to a restful API via Data Bus
        /// </summary>
        /// <param name="bindingExecution"></param>
        /// <param name="binding"></param>
        /// <param name="entity"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<long> TransformDataAsync(
            BindingExecution bindingExecution,
            Binding binding,
            Entity entity,
            CancellationToken cancellationToken)
        {
            bindingExecution.CheckWhetherArgumentIsNull(nameof(bindingExecution));
            binding.CheckWhetherArgumentIsNull(nameof(binding));
            entity.CheckWhetherArgumentIsNull(nameof(entity));

            try
            {
                this.LogDebug($"Entering HierarchicalDataTransformer.TransformDataAsync(BindingId = {binding.Id})", bindingExecution);

                HierarchicalConfiguration config = await this.GetConfiguration(binding, bindingExecution, entity);
                this.LogDebug($"Plugin Configuration: {Serialize(config)}", bindingExecution);

                JobData jobData = await this.GetJobData(binding, bindingExecution, entity);
                this.LogDebug($"JobData: {Serialize(jobData)}", bindingExecution);

                return await this.RunDatabusAsync(config, jobData, cancellationToken);
            }
            catch (Exception e)
            {
                this.LogError($"HierarchicalDataTransformer.TransformDataAsync threw an exception: {e}", e, bindingExecution);
                throw;
            }
        }

        /// <summary>
        /// <see cref="IDataTransformer.CanHandle"/>
        /// </summary>
        /// <param name="bindingExecution"></param>
        /// <param name="binding"></param>
        /// <param name="destinationEntity"></param>
        /// <returns></returns>
        public bool CanHandle(BindingExecution bindingExecution, Binding binding, Entity destinationEntity)
        {
            Binding topMost;
            try
            {
                Binding[] allBindings = this.GetBindingsForEntityAsync(destinationEntity).Result;
                topMost = this.GetTopMostBinding(allBindings);
            }
            catch (Exception e)
            {
                this.LogError(
                    $"HierarchicalDataTransformer.CanHandle threw exception for binding [{Serialize(binding)}], bindingExecution [{Serialize(bindingExecution)}",
                    e,
                    bindingExecution);
                throw;
            }

            return binding.BindingType == NestedBindingTypeName 
                   && binding.Id == topMost.Id 
                   && binding.SourceConnection.DataSystemTypeCode == DataSystemTypeCode.SqlServer;
        }

        private static void SetupSerilogLogger()
        {
            Log.Logger = CreateLogger<HierarchicalDataTransformer>();
        }

        private static ILogger CreateLogger<T>()
        {
            levelSwitch.MinimumLevel = LogEventLevel.Warning;
            return new LoggerConfiguration().WriteTo.File(
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs\\Plugins\\HierarchicalDataTransformer\\HierarchicalDataTransformer.log"),
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] - [{SourceContext}] - {Message}{NewLine}{Exception}", 
                    shared: true)
                .MinimumLevel.ControlledBy(levelSwitch)
                .CreateLogger().ForContext<T>();
        }

        private static void SwitchLogLevel(string newLogLevel)
        {
            if (newLogLevel.IsNullOrWhiteSpace())
            {
                return;
            }

            switch (newLogLevel)
            {
                case LogLevel.Verbose:
                    levelSwitch.MinimumLevel = LogEventLevel.Verbose;
                    break;
                case LogLevel.Debug:
                    levelSwitch.MinimumLevel = LogEventLevel.Debug;
                    break;
                case LogLevel.Information:
                    levelSwitch.MinimumLevel = LogEventLevel.Information;
                    break;
                case LogLevel.Warning:
                    levelSwitch.MinimumLevel = LogEventLevel.Warning;
                    break;
                case LogLevel.Error:
                    levelSwitch.MinimumLevel = LogEventLevel.Error;
                    break;
                case LogLevel.Fatal:
                    levelSwitch.MinimumLevel = LogEventLevel.Fatal;
                    break;
                default:
                    throw new ArgumentException(Resources.InvalidLogLevel.FormatCurrentCulture(newLogLevel));
            }
        }

        private static string Serialize(object obj)
        {
            return JsonConvert.SerializeObject(obj, new JsonSerializerSettings { ReferenceLoopHandling = ReferenceLoopHandling.Ignore });
        }

        private Binding GetTopMostBinding(Binding[] bindings)
        {
            if (bindings == null || bindings.Length == 0)
            {
                throw new InvalidOperationException("Could not get top most binding from a list with no bindings");
            }

            return bindings.First(binding => !this.GetAncestorObjectRelationships(binding, bindings).Any());
        }

        /// <summary>
        /// Gets configuration from config file (json) unless specified in attributes on the binding or entity
        /// </summary>
        /// <param name="binding"></param>
        /// <param name="bindingExecution"></param>
        /// <param name="entity"></param>
        /// <returns></returns>
        private async Task<HierarchicalConfiguration> GetConfiguration(Binding binding, BindingExecution bindingExecution, Entity entity)
        {
            string directoryName = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            if (directoryName == null)
            {
                throw new InvalidOperationException("Could not find plugin configuration file base path.");
            }

            string fullPath = Path.Combine(directoryName, Resources.DefaultConfigFileName);
            string json = File.ReadAllText(fullPath);
            dynamic deserialized = JsonConvert.DeserializeObject(json);
            dynamic databusConfiguration = deserialized.DatabusConfiguration;

            SwitchLogLevel(await this.GetPluginLogLevelSystemAttributeValue() ?? (string)databusConfiguration.LogLevel);

            var queryConfig = new QueryConfig
                                  {
                                      ConnectionString = this.BuildConnectionString(binding),
                                      Url = this.BuildUrl(entity) ?? databusConfiguration.Url,
                                      MaximumEntitiesToLoad = this.GetAttributeInt(binding.AttributeValues, AttributeNames.MaxEntitiesToLoad)
                                                              ?? databusConfiguration.MaximumEntitiesToLoad,
                                      EntitiesPerBatch = this.GetAttributeInt(binding.AttributeValues, AttributeNames.EntitiesPerBatch)
                                                         ?? databusConfiguration.EntitiesPerBatch,
                                      EntitiesPerUploadFile = this.GetAttributeInt(binding.AttributeValues, AttributeNames.EntitiesPerUploadFile)
                                                              ?? databusConfiguration.EntitiesPerUploadFile,
                                      LocalSaveFolder = this.BuildLocalSaveFolder(
                                                            binding.AttributeValues.GetAttributeTextValue(AttributeNames.LocalSaveFolder),
                                                            binding,
                                                            bindingExecution) 
                                                        ?? this.BuildLocalSaveFolder(Convert.ToString(databusConfiguration.LocalSaveFolder), binding, bindingExecution),
                                      WriteTemporaryFilesToDisk = this.GetAttributeBool(binding.AttributeValues, AttributeNames.WriteTempFilesToDisk) 
                                                                  ?? databusConfiguration.WriteTemporaryFilesToDisk,
                                      WriteDetailedTemporaryFilesToDisk = this.GetAttributeBool(binding.AttributeValues, AttributeNames.DetailedTempFiles)
                                                                          ?? databusConfiguration.WriteDetailedTemporaryFilesToDisk,
                                      CompressFiles = this.GetAttributeBool(binding.AttributeValues, AttributeNames.CompressFiles) 
                                                      ?? databusConfiguration.CompressFiles,
                                      UploadToUrl = this.GetAttributeBool(binding.AttributeValues, AttributeNames.UploadToUrl) 
                                                    ?? databusConfiguration.UploadToUrl,
                                      UrlMethod = this.GetHtmlMethod(entity.AttributeValues?.GetAttributeTextValue(AttributeNames.HttpMethod))
                                                  ?? this.GetHtmlMethod(Convert.ToString(databusConfiguration.UrlMethod)) 
                                                  ?? HttpMethod.Post
                                  };

            var hierarchicalConfig = new HierarchicalConfiguration
                                         {
                                             ClientSpecificConfiguration = this.GetClientSpecificConfiguration(entity, deserialized.ClientSpecificConfigurations),
                                             DatabusConfiguration = queryConfig
                                         };
            
            return hierarchicalConfig;
        }

        private IClientSpecificConfiguration GetClientSpecificConfiguration(Entity entity, dynamic clientSpecificConfigurationsSection)
        {
            string clientSpecificConfigurationKey = entity.Connection?.AttributeValues?.GetAttributeTextValue(AttributeNames.ClientSpecificConfigurationKey);

            if (!clientSpecificConfigurationKey.IsNullOrWhiteSpace())
            {
                string[] configObjPath = clientSpecificConfigurationKey.Split('.');

                dynamic allConfigurations = clientSpecificConfigurationsSection;

                dynamic selectedConfiguration = allConfigurations;
                foreach (string pathPart in configObjPath)
                {
                    // dig through the json structure to find the correct configuration
                    selectedConfiguration = selectedConfiguration[pathPart];
                    
                    if (selectedConfiguration == null)
                    {
                        throw new InvalidOperationException(Resources.CannotRetrieveClientSpecificConfiguration.FormatCurrentCulture(AttributeNames.ClientSpecificConfigurationKey, clientSpecificConfigurationKey));
                    }
                }

                IDictionary<string, object> configValues = JsonConvert.DeserializeObject<CaseInsensitiveDictionary<object>>(((JObject)selectedConfiguration).ToString());

                /* TODO - This needs to be fixed to no longer refer to UPMC anywhere in the plugin.
                          Ideally, there would be a module in this plugin that allows different clients 
                          to include their own specific logic around things like authentication.  
                          Then we could move the UpmcHmacAuthorizationRequestInterceptor and the UpmcSpecificConfiguration
                          out of the plugin and find a way to dynamically pick them up in the following if-block.
                          Also see To-Do in RunDataBus method if-block around upmcSpecificConfiguration.
                */
                if (clientSpecificConfigurationKey.CaseInsensitiveContains("upmc"))
                {
                    return new UpmcSpecificConfiguration(configValues);
                }
            }

            return null;
        }

        private async Task<string> GetPluginLogLevelSystemAttributeValue()
        {
            ObjectAttributeValue logLevelAttribute = await this.metadataServiceClient.GetSystemAttributeAsync(AttributeNames.HierarchicalPluginLogLevel);

            return logLevelAttribute?.AttributeValue;
        }

        private string BuildConnectionString(Binding binding)
        {
            string connectionString = binding.SourceConnection.AttributeValues?.GetAttributeTextValue(AttributeNames.ConnectionString);

            if (connectionString.IsNullOrWhiteSpace())
            {
                if (binding.SourceConnection.Server.IsNullOrWhiteSpace() || binding.SourceConnection.Database.IsNullOrWhiteSpace())
                {
                    throw new ArgumentException(Resources.CannotBuildConnectionString);
                }

                connectionString = Resources.SqlServerConnectionString.FormatCurrentCulture(binding.SourceConnection.Server, binding.SourceConnection.Database);
            }

            return connectionString;
        }

        private string BuildUrl(Entity entity)
        {
            string serviceUrl = entity?.Connection?.AttributeValues?.GetAttributeTextValue(AttributeNames.ServiceUrl);
            string endpoint = entity?.AttributeValues?.GetAttributeTextValue(AttributeNames.Endpoint);

            if (serviceUrl == null || endpoint == null)
            {
                return null;
            }

            return string.Join("/", serviceUrl, endpoint);
        }

        private string BuildLocalSaveFolder(string baseUrl, Binding binding, BindingExecution bindingExecution)
        {
            if (baseUrl.IsNullOrWhiteSpace())
            {
                return null;
            }

            return Path.Combine(baseUrl, $"{binding.Name}_{bindingExecution.BindingId}_{bindingExecution.Id}");
        }

        private int? GetAttributeInt(ICollection<ObjectAttributeValue> attributes, string attributeName)
        {
            int value;
            if (int.TryParse(attributes.GetAttributeTextValue(attributeName), out value))
            {
                return value;
            }

            return null;
        }

        private bool? GetAttributeBool(ICollection<ObjectAttributeValue> attributes, string attributeName)
        {
            bool value;
            if (bool.TryParse(attributes.GetAttributeTextValue(attributeName), out value))
            {
                return value;
            }

            return null;
        }

        private async Task<JobData> GetJobData(Binding binding, BindingExecution bindingExecution, Entity destinationEntity)
        {
            var jobData = new JobData();

            Binding[] allBindings = await this.GetBindingsForEntityAsync(destinationEntity);

            this.ValidateHierarchicalBinding(binding, allBindings);

            List<DataSource> dataSources = new List<DataSource>();

            await this.GenerateDataSources(binding, bindingExecution, allBindings, destinationEntity, dataSources, null, "$", isFirst: true);

            var jobDataTopLevelDataSource = dataSources.First();
            jobData.TopLevelDataSource = jobDataTopLevelDataSource as TopLevelDataSource;
            jobData.MyDataSources = dataSources.ToList();

            return jobData;
        }

        /// <summary>
        /// Execute DataBus with the given configuration and job data
        /// </summary>
        /// <param name="config"></param>
        /// <param name="jobData"></param>
        /// <param name="cancellationToken"></param>
        private async Task<long> RunDatabusAsync(
            HierarchicalConfiguration config,
            JobData jobData,
            CancellationToken cancellationToken)
        {
            var container = new UnityContainer();

            // TODO - need to figure out a way to remove anything UPMC-specific from this plugin.
            UpmcSpecificConfiguration upmcSpecificConfiguration = (UpmcSpecificConfiguration)config.ClientSpecificConfiguration;
            if (upmcSpecificConfiguration != null)
            {
                container.RegisterInstance<IHttpRequestInterceptor>(
                    new UpmcHmacAuthorizationRequestInterceptor(upmcSpecificConfiguration.AppId, upmcSpecificConfiguration.AppSecret, upmcSpecificConfiguration.TenantSecret));
            }

            var rowCounter = new RowCounterBatchEventsLogger();
            ILogger databusLogger = CreateLogger<DatabusRunner>();

            container.RegisterInstance(databusLogger);
            container.RegisterInstance<IBatchEventsLogger>(rowCounter);

            var jobEventsLogger = new JobEventsLogger();
            container.RegisterInstance<IJobEventsLogger>(jobEventsLogger);
            container.RegisterInstance<IQuerySqlLogger>(new QuerySqlLogger());
            container.RegisterInstance<IHttpResponseLogger>(new MyHttpResponseLogger());

            var job = new Job { Config = config.DatabusConfiguration, Data = jobData };

            this.LogDebug($"Executing DatabusRunner.RunRestApiPipeline with:\n\tcontainer: {Serialize(container)}\n\tjob: {Serialize(job)}");

            try
            {
                await this.runner.RunRestApiPipelineAsync(container, job, cancellationToken);
            }
            catch (AggregateException e)
            {
                this.LogError($"Databus threw an error: {e}", e);
                throw e.Flatten();
            }

            SetupSerilogLogger(); // re-setup logger as Databus is closing it
            this.LogDebug($"Databus execution complete.  Processed { jobEventsLogger.NumberOfEntities } records.");
            return jobEventsLogger.NumberOfEntities;
        }

        private void ValidateHierarchicalBinding(Binding binding, Binding[] allBindings)
        {
            if (!allBindings.All(b => b.SourcedByEntities.Count == 1 && b.SourcedByEntities.FirstOrDefault() != null))
            {
                throw new InvalidOperationException("All bindings must have exactly 1 sourced entity");
            }

            if (this.GetAncestorObjectRelationships(binding, allBindings).Any())
            {
                throw new InvalidOperationException("Top-most binding cannot have any ancestor bindings");
            }

            if (allBindings.Any(b => !this.GetAncestorObjectRelationships(b, allBindings).Any() && !this.GetChildObjectRelationships(b).Any()))
            {
                throw new InvalidOperationException("Each binding must be in a relationship");
            }

            // TODO: Innovation Time -- validate that tree is not disjointed
        }

        private async Task GenerateDataSources(
            Binding rootBinding,
            BindingExecution bindingExecution,
            Binding[] allBindings,
            Entity destinationEntity,
            List<DataSource> dataSources,
            ObjectReference relationshipToParent,
            string path,
            bool isFirst)
        {
            Entity sourceEntity = await this.GetEntityFromBinding(rootBinding);
            Field[] sourceEntityFields = await this.metadataServiceClient.GetEntityFieldsAsync(sourceEntity);

            if (isFirst)
            {
                dataSources.Add(
                    new TopLevelDataSource
                        {
                            Path = path,
                            Key = this.GetKeyColumnsAsCsv(sourceEntity, sourceEntityFields),
                            TableOrView = this.GetFullyQualifiedTableName(sourceEntity),
                            MySqlEntityColumnMappings =
                                await this.GetColumnsFromEntity(sourceEntity, destinationEntity),
                            PropertyType = null,
                            MyRelationships = new List<SqlRelationship>(),
                            MyIncrementalColumns = this.GetIncrementalConfigurations(rootBinding, bindingExecution, sourceEntityFields)
                        });
            }
            else
            {
                dataSources.Add(
                    new DataSource
                        {
                            Path = path,
                            TableOrView = this.GetFullyQualifiedTableName(sourceEntity),
                            MySqlEntityColumnMappings =
                                await this.GetColumnsFromEntity(sourceEntity, destinationEntity),
                            PropertyType = this.GetCardinalityFromObjectReference(relationshipToParent),
                            MyRelationships = await this.GetDatabusRelationships(rootBinding, allBindings, sourceEntity)
                        });
            }

            List<ObjectReference> childObjectRelationships = this.GetChildObjectRelationships(rootBinding);
            bool hasChildren = childObjectRelationships.Count > 0;
            if (!hasChildren)
            {
                return;
            }
            
            foreach (ObjectReference childObjectRelationship in childObjectRelationships)
            {
                if (childObjectRelationship != null)
                {
                    Binding childBinding = this.GetMatchingChild(allBindings, childObjectRelationship.ChildObjectId);
                    await this.GenerateDataSources(
                        childBinding,
                        bindingExecution, 
                        allBindings,
                        destinationEntity,
                        dataSources,
                        childObjectRelationship,
                        string.Join(".", path, await this.GetSourceAliasName(childBinding)),
                        isFirst: false);
                }
            }
        }

        private List<IncrementalColumn> GetIncrementalConfigurations(Binding binding, BindingExecution bindingExecution, Field[] sourceEntityFields)
        {
            this.LogDebug("Checking Incremental Configurations", bindingExecution);

            var incrementalColumns = new List<IncrementalColumn>();

            if (bindingExecution.LoadType != BindingLoadType.Incremental)
            {
                return incrementalColumns;
            }

            if (binding.IncrementalConfigurations.Count == 0)
            {
                return incrementalColumns;
            }

            foreach (IncrementalConfiguration incrementalConfiguration in binding.IncrementalConfigurations)
            {
                this.LogDebug($"Processing incremental configuration: {Serialize(incrementalConfiguration)} \n\tfor binding: {Serialize(bindingExecution)}");

                IncrementalValue incrementalValue;
                if (bindingExecution.BatchExecution.IncrementalStartDateTime.HasValue)
                {
                    incrementalValue = new IncrementalValue
                                           {
                                               DestinationBindingId = bindingExecution.BindingId,
                                               LastMaxIncrementalDate = bindingExecution.BatchExecution.IncrementalStartDateTime
                                           };
                }
                else
                {
                    using (IProcessingContextWrapper processingContextWrapper = this.processingContextWrapperFactory.CreateProcessingContextWrapper())
                    {
                        incrementalValue = processingContextWrapper.GetIncrementalValue(incrementalConfiguration);
                    }
                }

                this.LogDebug($"Found incrementalValue: {Serialize(incrementalValue)}");

                if (incrementalValue?.LastMaxIncrementalDate == null)
                {
                    continue;
                }

                incrementalColumns.Add(new IncrementalColumn
                                           {
                                               Name = incrementalConfiguration.IncrementalColumnName,
                                               Operator = IncrementalOperator.GreaterThanOrEqualTo,
                                               Type = sourceEntityFields.First(f => f.FieldName == incrementalConfiguration.IncrementalColumnName).DataType,
                                               Value = Serialize(incrementalValue.LastMaxIncrementalDate.Value).Replace("\"", string.Empty)
                                           });
            }

            this.LogDebug($"processed the following incrementalColumns: {Serialize(incrementalColumns)}");

            return incrementalColumns;
        }

        private string GetKeyColumnsAsCsv(Entity sourceEntity, Field[] sourceEntityFields)
        {
            List<string> list = sourceEntityFields.Where(field => field.IsPrimaryKey).Select(field => field.FieldName).ToList();

            if (!list.Any())
            {
                throw new Exception($"No primary keys found for entity {sourceEntity.EntityName}");
            }

            return string.Join(",", list);
        }

        private async Task<string> GetSourceAliasName(Binding binding)
        {
            return binding?.SourcedByEntities?.FirstOrDefault()?.SourceAliasName ?? (await this.GetEntityFromBinding(binding)).EntityName;
        }

        private async Task<List<SqlRelationship>> GetDatabusRelationships(Binding binding, Binding[] allBindings, Entity sourceEntity)
        {
            List<BindingReference> ancestorRelationships = this.GetAncestorObjectRelationships(binding, allBindings);
            List<SqlRelationship> sqlRelationships = new List<SqlRelationship>();

            foreach (BindingReference ancestorRelationship in ancestorRelationships)
            {
                Entity ancestorEntity = await this.GetEntityFromBinding(allBindings.First(b => b.Id == ancestorRelationship.ParentObjectId));

                sqlRelationships.Add(
                    new SqlRelationship
                        {
                            MySource = new SqlRelationshipEntity
                                           {
                                               Entity = this.GetFullyQualifiedTableName(ancestorEntity),
                                               Key = this.CleanJson(
                                                   ancestorRelationship.AttributeValues.GetAttributeTextValue(AttributeNames.ParentKeyFields))
                                           }, 
                            MyDestination =
                                new SqlRelationshipEntity
                                    {
                                        Entity = this.GetFullyQualifiedTableName(sourceEntity),
                                        Key = this.CleanJson(ancestorRelationship.AttributeValues.GetAttributeTextValue(AttributeNames.ChildKeyFields))
                                    } 
                        });
            }

            return sqlRelationships;
        }

        private async Task<Binding[]> GetBindingsForEntityAsync(Entity entity)
        {
            Binding[] bindingsForDataMart = await this.metadataServiceClient.GetBindingsForDataMartAsync(entity.DataMartId);

            return bindingsForDataMart.Where(binding => binding.DestinationEntityId == entity.Id).ToArray();
        }

        private string GetCardinalityFromObjectReference(ObjectReference objectReference)
        {
            return this.GetAttributeValueFromObjectReference(objectReference, AttributeNames.Cardinality).Equals("array", StringComparison.CurrentCultureIgnoreCase) ? "array" : "object";
        }

        private string GetAttributeValueFromObjectReference(ObjectReference objectReference, string attributeName)
        {
            return objectReference.AttributeValues.Where(x => x.AttributeName == attributeName)
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        private Binding GetMatchingChild(Binding[] bindings, int childBindingId)
        {
            return bindings.FirstOrDefault(x => x.Id == childBindingId);
        }

        private List<ObjectReference> GetChildObjectRelationships(Binding binding)
        {
            List<ObjectReference> childRelationships = binding.ObjectRelationships.Where(
                    or => or.ChildObjectType == MetadataObjectType.Binding
                          && or.AttributeValues.First(attr => attr.AttributeName == AttributeNames.GenerationGap).ValueToInt()
                          == 1)
                .ToList();
            Log.Logger.Verbose($"Found child object relationships for binding with id = {binding.Id}: {Serialize(childRelationships)}");
            return childRelationships;
        }

        private List<BindingReference> GetAncestorObjectRelationships(Binding binding, Binding[] allBindings)
        {
            var parentRelationships = new List<BindingReference>();
            foreach (Binding otherBinding in allBindings.Where(b => b.Id != binding.Id))
            {
                parentRelationships.AddRange(
                    otherBinding.ObjectRelationships.Where(
                        relationship => relationship.ChildObjectId == binding.Id && relationship.ChildObjectType == MetadataObjectType.Binding).Select(
                        x => new BindingReference
                        {
                            ChildObjectId = x.ChildObjectId,
                            AttributeValues = x.AttributeValues,
                            ChildObjectType = x.ChildObjectType,
                            ParentObjectId = otherBinding.Id
                        }));
            }

            Log.Logger.Verbose($"Found ancestor object relationships for binding with id = {binding.Id}: {Serialize(parentRelationships)}");
            return parentRelationships;
        }

        private async Task<Entity> GetEntityFromBinding(Binding binding)
        {
            if (binding == null || !binding.SourcedByEntities.Any() || binding.SourcedByEntities.FirstOrDefault() == null)
            {
                return null;
            }

            SourceEntityReference entityReference = binding.SourcedByEntities.First();
            Entity entity = await this.metadataServiceClient.GetEntityAsync(entityReference.SourceEntityId);
            Log.Logger.Verbose($"Found source destinationEntity ({entity.EntityName}) for binding (id = {binding.Id})");
            return entity;
        }

        private async Task<List<SqlEntityColumnMapping>> GetColumnsFromEntity(Entity sourceEntity, Entity destinationEntity)
        {
            if (sourceEntity == null || destinationEntity == null)
            {
                return null;
            }

            List<SqlEntityColumnMapping> columns = new List<SqlEntityColumnMapping>();

            Field[] sourceEntityFields = await this.metadataServiceClient.GetEntityFieldsAsync(sourceEntity);

            // Add all Active fields (based on destination entity)
            columns.AddRange(
                sourceEntityFields
                    .Where(
                        field => destinationEntity.Fields.Any(
                            destinationField => destinationField.FieldName == $"{sourceEntity.EntityName}{SourceEntitySourceColumnSeparator}{field.FieldName}" && destinationField.Status != FieldStatus.Omitted))
                    .Select(f => new SqlEntityColumnMapping { Name = f.FieldName }));

            return columns;
        }

        private string GetFullyQualifiedTableName(Entity sourceEntity)
        {
            return $"[{sourceEntity.DatabaseName}].[{sourceEntity.SchemaName}].[{sourceEntity.EntityName}]";
        }

        private string CleanJson(string dirty)
        {
            return dirty.Replace("[", string.Empty).Replace("]", string.Empty).Replace('"', ' ').Trim();
        }

        private void LogDebug(string message, BindingExecution bindingExecution = null)
        {
            Log.Logger.Debug(message);

            if (bindingExecution != null)
            {
                this.loggingRepository.LogInformation(bindingExecution, message);
            }
        }

        private void LogError(string message, Exception e, BindingExecution bindingExecution = null)
        {
            Log.Logger.Error(message, e);

            if (bindingExecution != null)
            {
                this.loggingRepository.LogError(bindingExecution, e);
            }
        }

        private HttpMethod GetHtmlMethod(string methodName)
        {
            if (methodName.IsNullOrWhiteSpace())
            {
                return null;
            }

            HttpMethod urlMethod;
            switch (methodName)
            {
                case nameof(HttpMethod.Put):
                    urlMethod = HttpMethod.Put;
                    break;
                case nameof(HttpMethod.Post):
                    urlMethod = HttpMethod.Post;
                    break;
                default:
                    throw new ArgumentException(Resources.InvalidHttpMethod.FormatCurrentCulture(methodName));
            }

            return urlMethod;
        }

        private static class MetadataObjectType
        {
            public const string Binding = "Binding";
        }

        private static class IncrementalOperator
        {
            public const string GreaterThanOrEqualTo = "GreaterThanOrEqualTo";
        }

        private static class LogLevel
        {
            public const string Verbose = "Verbose";

            public const string Debug = "Debug";

            public const string Information = "Information";

            public const string Warning = "Warning";

            public const string Error = "Error";

            public const string Fatal = "Fatal";
        }
    }
}
