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
    using System.Reflection;
    using System.Threading;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Context;

    using DataConverter.Loggers;

    using Fabric.Databus.Client;
    using Fabric.Databus.Config;
    using Fabric.Databus.Interfaces.Loggers;
    using Fabric.Shared.ReliableHttp.Interfaces;

    using Newtonsoft.Json;

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

        private readonly DatabusRunner runner;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformer"/> class.
        /// </summary>
        /// <param name="metadataServiceClient"></param>
        /// <param name="processingContextWrapperFactory"></param>
        public HierarchicalDataTransformer(IMetadataServiceClient metadataServiceClient, IProcessingContextWrapperFactory processingContextWrapperFactory)
        {
            this.metadataServiceClient = metadataServiceClient ?? throw new ArgumentException("metadataServiceClient cannot be null.");
            this.processingContextWrapperFactory = processingContextWrapperFactory ?? throw new ArgumentException("ProcessingContextWrapperFactory cannot be null.");

            this.runner = new DatabusRunner();

            LoggingHelper2.Debug("Created instance of HierarchicalDataTransformer");
        }

        /// <summary>
        /// The transform data async.
        /// </summary>
        /// <param name="bindingExecution">
        /// The binding execution.
        /// </param>
        /// <param name="binding">
        /// The binding.
        /// </param>
        /// <param name="entity">
        /// The entity.
        /// </param>
        /// <param name="cancellationToken">
        /// The cancellation token.
        /// </param>
        /// <returns>
        /// The <see cref="Task"/>.
        /// </returns>
        public async Task<long> TransformDataAsync(
            BindingExecution bindingExecution,
            Binding binding,
            Entity entity,
            CancellationToken cancellationToken)
        {
            try
            {
                LoggingHelper2.Debug("In TransformDataAsync()");
                HierarchicalConfiguration config = this.GetConfigurationFromJsonFile();
                LoggingHelper2.Debug($"Configuration: {JsonConvert.SerializeObject(config)}");

                JobData jobData = await this.GetJobData(binding, bindingExecution, entity);
                LoggingHelper2.Debug($"JobData: {JsonConvert.SerializeObject(jobData)}");

                this.RunDatabus(config, jobData);
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"TransformDataAsync Threw exception: {e}");
                throw;
            }

            return Convert.ToInt64(1);
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
                LoggingHelper2.Debug($"Threw exception: {e}");
                throw;
            }

            // check the binding to see whether it has a destination entity
            // where it has an endpoint attribute, httpverb
            return binding.BindingType == NestedBindingTypeName && binding.Id == topMost.Id; // BindingType.
        }

        private Binding GetTopMostBinding(Binding[] bindings)
        {
            if (bindings == null || bindings.Length == 0)
            {
                LoggingHelper2.Debug("ERROR - Throwing exception: Could not get top most binding from a list with no bindings");
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
                                      UploadToUrl = databusConfiguration.UploadToUrl
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
        private void RunDatabus(HierarchicalConfiguration config, JobData jobData)
        {
            LoggingHelper2.Debug("We are trying to run Databus");
            var job = new Job
                          {
                              Config = config.DatabusConfiguration,
                              Data = jobData,
                          };
            try
            {
                UpmcSpecificConfig upmcSpecificConfig = (UpmcSpecificConfig)config.ClientSpecificConfiguration;
                var container = new UnityContainer();
                container.RegisterInstance<IHttpRequestInterceptor>(
                    new HmacAuthorizationRequestInterceptor(
                        upmcSpecificConfig.AppId,
                        upmcSpecificConfig.AppSecret,
                        upmcSpecificConfig.TenantId,
                        upmcSpecificConfig.TenantSecret));

                container.RegisterInstance<IBatchEventsLogger>(new BatchEventsLogger());
                var jobEventsLogger = new JobEventsLogger();
                container.RegisterInstance<IJobEventsLogger>(jobEventsLogger);
                container.RegisterInstance<IQuerySqlLogger>(new QuerySqlLogger());

                this.runner.RunRestApiPipeline(container, job, new CancellationToken());

                int numberOfEntitiesProcessed = jobEventsLogger.NumberOfEntities;
            }
            catch (AggregateException e)
            {
                foreach (var innerException in e.Flatten().InnerExceptions)
                {
                    var nestedInnerException = innerException;
                    do
                    {
                        if (!string.IsNullOrEmpty(nestedInnerException.Message))
                        {
                            Console.WriteLine(nestedInnerException.Message);
                        }

                        nestedInnerException = nestedInnerException.InnerException;
                    }
                    while (nestedInnerException != null);
                }

                throw e.Flatten();
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"Exception thrown by Databus: {e}");
                throw;
            }

            LoggingHelper2.Debug("Finished executing Databus");
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
                                await this.GetColumnsFromEntity(sourceEntity, destinationEntity, rootBinding.SourcedByEntities.First().SourceAliasName),
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
                                await this.GetColumnsFromEntity(sourceEntity, destinationEntity, rootBinding.SourcedByEntities.First().SourceAliasName),
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
            LoggingHelper2.Debug($"IncrementalConfigurations: {JsonConvert.SerializeObject(binding.IncrementalConfigurations)}");
            LoggingHelper2.Debug($"MaxObservedIncrementalDate: {JsonConvert.SerializeObject(bindingExecution.MaxObservedIncrementalDate)}");
            var incrementalColumns = new List<IncrementalColumn>();

            if (binding.IncrementalConfigurations.Count == 0)
            {
                return incrementalColumns;
            }

            foreach (IncrementalConfiguration incrementalConfiguration in binding.IncrementalConfigurations)
            {
                LoggingHelper2.Debug($"IncrementalStartDateTime: {JsonConvert.SerializeObject(bindingExecution.BatchExecution.IncrementalStartDateTime)}");

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

                LoggingHelper2.Debug($"incrementalValue: {JsonConvert.SerializeObject(incrementalValue)}");

                if (incrementalValue?.LastMaxIncrementalDate == null)
                {
                    continue;
                }

                incrementalColumns.Add(new IncrementalColumn
                                           {
                                               Name = incrementalConfiguration.IncrementalColumnName,
                                               Operator = IncrementalOperator.GreaterThanOrEqualTo,
                                               Type = sourceEntityFields.First(f => f.FieldName == incrementalConfiguration.IncrementalColumnName).DataType,
                                               Value = JsonConvert.SerializeObject(incrementalValue.LastMaxIncrementalDate.Value).Replace("\"", string.Empty)
                                           });
            }

            LoggingHelper2.Debug($"incrementalColumns: {JsonConvert.SerializeObject(incrementalColumns)}");

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
            LoggingHelper2.Debug("Entering GetCardinalityFromObjectReference(...)");
            return this.GetAttributeValueFromObjectReference(objectReference, AttributeName.Cardinality).Equals("array", StringComparison.CurrentCultureIgnoreCase) ? "array" : "object";
        }

        private string GetAttributeValueFromObjectReference(ObjectReference objectReference, string attributeName)
        {
            LoggingHelper2.Debug("Entering GetAttributeValueFromObjectReference(...)");
            LoggingHelper2.Debug($"attributeName: {attributeName}");

            return objectReference.AttributeValues.Where(x => x.AttributeName == attributeName)
                .Select(x => x.AttributeValue).FirstOrDefault();
        }

        private Binding GetMatchingChild(Binding[] bindings, int childBindingId)
        {
            LoggingHelper2.Debug("Entering GetMatchingChild(...)");
            return bindings.FirstOrDefault(x => x.Id == childBindingId);
        }

        private List<ObjectReference> GetChildObjectRelationships(Binding binding)
        {
            LoggingHelper2.Debug("Entering GetChildObjectRelationships(...)");
            List<ObjectReference> childRelationships = binding.ObjectRelationships.Where(
                    or => or.ChildObjectType == MetadataObjectType.Binding
                          && or.AttributeValues.First(attr => attr.AttributeName == AttributeName.GenerationGap).ValueToInt()
                          == 1)
                .ToList();

            return childRelationships;
        }

        private List<BindingReference> GetAncestorObjectRelationships(Binding binding, Binding[] allBindings)
        {
            LoggingHelper2.Debug("Entering GetAncestorObjectRelationships(...)");
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

            return parentRelationships;
        }

        private async Task<Entity> GetEntityFromBinding(Binding binding)
        {
            LoggingHelper2.Debug("Entering GetEntityFromBinding(...)");

            if (binding == null || !binding.SourcedByEntities.Any() || binding.SourcedByEntities.FirstOrDefault() == null)
            {
                return null;
            }

            SourceEntityReference entityReference = binding.SourcedByEntities.First();
            Entity entity = await this.metadataServiceClient.GetEntityAsync(entityReference.SourceEntityId);
            LoggingHelper2.Debug($"Found source destinationEntity ({entity.EntityName}) for binding (id = {binding.Id})");
            return entity;
        }

        private async Task<List<SqlEntityColumnMapping>> GetColumnsFromEntity(Entity sourceEntity, Entity destinationEntity, string entityAlias)
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
                    .Select(f => new SqlEntityColumnMapping { Name = f.FieldName, Alias = entityAlias ?? f.FieldName }));

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
