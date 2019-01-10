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

    using DataConverter.Loggers;

    using Fabric.Databus.Client;
    using Fabric.Databus.Config;
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Shared.ReliableHttp.Interfaces;

    using Newtonsoft.Json;

    using Serilog;

    using Unity;

    /// <summary>
    /// The hierarchical data transformer.
    /// </summary>
    public class HierarchicalDataTransformer : IDataTransformer
    {
        public const string NestedBindingTypeName = "Nested";
        private const string SourceEntitySourceColumnSeparator = "__";

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
            try
            {
                this.LogDebug(
                    string.Join(
                        "\n\t",
                        "Entering HierarchicalDataTransformer.TransformDataAsync with:",
                        $"bindingExecution: {Serialize(bindingExecution)}",
                        $"binding: {Serialize(binding)}",
                        $"entity: {Serialize(entity)}"),
                    bindingExecution);

                HierarchicalConfiguration config = this.GetConfigurationFromJsonFile();
                this.LogDebug($"HierarchicalConfiguration: {Serialize(config)}", bindingExecution);

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

            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpverb
            return binding.BindingType == NestedBindingTypeName && binding.Id == topMost.Id;
        }

        private static void SetupSerilogLogger()
        {
            Log.Logger = CreateLogger<HierarchicalDataTransformer>();
        }

        private static ILogger CreateLogger<T>()
        {
            return new LoggerConfiguration().WriteTo.File(
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs\\Plugins\\HierarchicalDataTransformer\\HierarchicalDataTransformer.log"),
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] - [{SourceContext}] - {Message}{NewLine}{Exception}", 
                    shared: true)
                .MinimumLevel.Information()
                .CreateLogger().ForContext<T>();
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

        private HierarchicalConfiguration GetConfigurationFromJsonFile(string filePath = "config.json")
        {
            string directoryName = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            if (directoryName == null)
            {
                throw new InvalidOperationException("Could not find plugin configuration file base path.");
            }

            string fullPath = Path.Combine(directoryName, filePath);
            string json = File.ReadAllText(fullPath);
            dynamic deserialized = JsonConvert.DeserializeObject(json);
            dynamic databusConfiguration = deserialized.DatabusConfiguration;
            
            var queryConfig = new QueryConfig
                                  {
                                      ConnectionString = databusConfiguration.ConnectionString,
                                      Url = databusConfiguration.Url,
                                      MaximumEntitiesToLoad = databusConfiguration.MaximumEntitiesToLoad,
                                      EntitiesPerBatch = databusConfiguration.EntitiesPerBatch,
                                      EntitiesPerUploadFile = databusConfiguration.EntitiesPerUploadFile,
                                      LocalSaveFolder = databusConfiguration.LocalSaveFolder,
                                      WriteTemporaryFilesToDisk = databusConfiguration.WriteTemporaryFilesToDisk,
                                      WriteDetailedTemporaryFilesToDisk = databusConfiguration.WriteDetailedTemporaryFilesToDisk,
                                      UploadToUrl = databusConfiguration.UploadToUrl,
                                      UrlMethod = this.GetHtmlMethod(Convert.ToString(databusConfiguration.UrlMethod))
            };

            dynamic upmcSpecificConfiguration = deserialized.ClientSpecificConfiguration;
            var upmcSpecificConfig = new UpmcSpecificConfig
                                          {
                                              Name = upmcSpecificConfiguration.name,
                                              AppId = upmcSpecificConfiguration.AppId,
                                              AppSecret = upmcSpecificConfiguration.AppSecret,
                                              BaseUrl = upmcSpecificConfiguration.BaseUrl,
                                              TenantId = upmcSpecificConfiguration.TenantId,
                                              TenantSecret = upmcSpecificConfiguration.TenantSecret
                                          };
            var hierarchicalConfig = new HierarchicalConfiguration
                                         {
                                             ClientSpecificConfiguration = upmcSpecificConfig,
                                             DatabusConfiguration = queryConfig
                                         };

            return hierarchicalConfig;
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
            var job = new Job { Config = config.DatabusConfiguration, Data = jobData };

            UpmcSpecificConfig upmcSpecificConfig = (UpmcSpecificConfig)config.ClientSpecificConfiguration;
            var rowCounter = new RowCounterBatchEventsLogger();
            ILogger databusLogger = CreateLogger<DatabusRunner>();

            var container = new UnityContainer();

            container.RegisterInstance<IHttpRequestInterceptor>(new HmacAuthorizationRequestInterceptor(
               upmcSpecificConfig.AppId,
               upmcSpecificConfig.AppSecret,
               upmcSpecificConfig.TenantId,
               upmcSpecificConfig.TenantSecret));
            container.RegisterInstance(databusLogger);
            container.RegisterInstance<IBatchEventsLogger>(rowCounter);

            var jobEventsLogger = new JobEventsLogger();
            container.RegisterInstance<IJobEventsLogger>(jobEventsLogger);
            container.RegisterInstance<IQuerySqlLogger>(new QuerySqlLogger());
            container.RegisterInstance<IHttpResponseLogger>(new MyHttpResponseLogger());

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
            this.LogDebug(
                string.Join(
                    "\n\t",
                    "Entering GetIncrementalConfigurations with: ",
                    $"binding: {Serialize(binding)}",
                    $"bindingExecution: {Serialize(bindingExecution)}",
                    $"sourceEntityFields: {Serialize(sourceEntityFields)}"),
                bindingExecution);

            var incrementalColumns = new List<IncrementalColumn>();

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
                                                   ancestorRelationship.AttributeValues.GetAttributeTextValue(AttributeName.ParentKeyFields))
                                           }, 
                            MyDestination =
                                new SqlRelationshipEntity
                                    {
                                        Entity = this.GetFullyQualifiedTableName(sourceEntity),
                                        Key = this.CleanJson(ancestorRelationship.AttributeValues.GetAttributeTextValue(AttributeName.ChildKeyFields))
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
            return this.GetAttributeValueFromObjectReference(objectReference, AttributeName.Cardinality).Equals("array", StringComparison.CurrentCultureIgnoreCase) ? "array" : "object";
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
                          && or.AttributeValues.First(attr => attr.AttributeName == AttributeName.GenerationGap).ValueToInt()
                          == 1)
                .ToList();
            this.LogDebug($"Found child object relationships for binding with id = {binding.Id}: {Serialize(childRelationships)}");
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

            this.LogDebug($"Found ancestor object relationships for binding with id = {binding.Id}: {Serialize(parentRelationships)}");
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
            this.LogDebug($"Found source destinationEntity ({entity.EntityName}) for binding (id = {binding.Id})");
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
            HttpMethod urlMethod;
            switch (methodName)
            {
                case nameof(HttpMethod.Put):
                    urlMethod = HttpMethod.Put;
                    break;
                default:
                    urlMethod = HttpMethod.Post;
                    break;
            }

            return urlMethod;
        }

        private static class MetadataObjectType
        {
            public const string Binding = "Binding";
        }

        private static class AttributeName
        {
            public const string Cardinality = "Cardinality";
            public const string ParentKeyFields = "ParentKeyFields";
            public const string ChildKeyFields = "ChildKeyFields";
            public const string GenerationGap = "GenerationGap";
        }

        private static class IncrementalOperator
        {
            public const string GreaterThanOrEqualTo = "GreaterThanOrEqualTo";
        }

    }
}
