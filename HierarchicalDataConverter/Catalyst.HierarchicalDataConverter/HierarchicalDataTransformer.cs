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

    using Fabric.Databus.Client;
    using Fabric.Databus.Config;
    using Fabric.Databus.Interfaces.Http;
    using Fabric.Shared.ReliableHttp.Interfaces;

    using Newtonsoft.Json;

    using Unity;

    /// <summary>
    /// The hierarchical data transformer.
    /// </summary>
    public class HierarchicalDataTransformer : IDataTransformer
    {
        private const string NestedBindingTypeName = "Nested";
        private const string PluginFolderName = "HierarchicalDataConverter"; // Plugin must be placed in this folder within the Plugins folder
        private const string SourceEntitySourceColumnSeparator = "__";

        /// <summary>
        /// The helper.
        /// </summary>
        private readonly IMetadataServiceClient metadataServiceClient;

        private readonly DatabusRunner runner;

        /// <summary>
        /// Initializes a new instance of the <see cref="HierarchicalDataTransformer"/> class.
        /// </summary>
        /// <param name="metadataServiceClient">
        /// The metadata Service Client.
        /// </param>
        public HierarchicalDataTransformer(IMetadataServiceClient metadataServiceClient)
        {
            this.metadataServiceClient = metadataServiceClient ?? throw new ArgumentException("metadataServiceClient cannot be null.");

            this.runner = new DatabusRunner();

            LoggingHelper2.Debug("We Got Here: HierarchicalDataTransformer!");
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
                var config = this.GetQueryConfigFromJsonFile();
                LoggingHelper2.Debug($"Configuration: {JsonConvert.SerializeObject(config)}");

                var jobData = await this.GetJobData(binding, entity);
                LoggingHelper2.Debug($"JobData: {JsonConvert.SerializeObject(jobData)}");

                this.RunDatabus(config, jobData);
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"TransformDataAsync Threw exception: {e}");
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
            var guid2 = Guid.NewGuid();

            Binding topMost;
            try
            {
                Binding[] allBindings = this.GetBindingsForEntityAsync(destinationEntity).Result;
                topMost = this.GetTopMostBinding(allBindings);
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"Threw exception ({guid2.ToString().Substring(0, 10)}): {e}");
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

        private QueryConfig GetQueryConfigFromJsonFile(string filePath = "config.json")
        {
            var directoryName = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);

            if (directoryName == null)
            {
                throw new InvalidOperationException("Could not find plugin configuration file base path.");
            }

            var fullPath = Path.Combine(directoryName, "Plugins", PluginFolderName, filePath);
            var json = File.ReadAllText(fullPath);
            var deserialized = (dynamic)JsonConvert.DeserializeObject(json);

            var queryConfig = new QueryConfig
                                  {
                                      ConnectionString = deserialized.ConnectionString,
                                      Url = deserialized.Url,
                                      MaximumEntitiesToLoad = deserialized.MaximumEntitiesToLoad,
                                      EntitiesPerBatch = deserialized.EntitiesPerBatch,
                                      EntitiesPerUploadFile = deserialized.EntitiesPerUploadFile,
                                      LocalSaveFolder = deserialized.LocalSaveFolder,
                                      WriteTemporaryFilesToDisk = deserialized.WriteTemporaryFilesToDisk,
                                      WriteDetailedTemporaryFilesToDisk = deserialized.WriteDetailedTemporaryFilesToDisk,
                                      UploadToUrl = deserialized.UploadToUrl
                                  };

            return queryConfig;
        }

        private async Task<JobData> GetJobData(Binding binding, Entity destinationEntity)
        {
            var jobData = new JobData();

            Binding[] allBindings = await this.GetBindingsForEntityAsync(destinationEntity);

            this.ValidateHierarchicalBinding(binding, allBindings);

            List<DataSource> dataSources = new List<DataSource>();

            await this.GenerateDataSources(binding, allBindings, destinationEntity, dataSources, null, "$", isFirst: true);

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
        private void RunDatabus(QueryConfig config, JobData jobData)
        {
            LoggingHelper2.Debug("We are trying to run Databus");
            var job = new Job
                          {
                              Config = config,
                              Data = jobData,
                          };
            try
            {
                // TODO: Get the authentication appId and secret from the database
                var container = new UnityContainer();
                container.RegisterInstance<IHttpRequestInterceptor>(new HmacAuthorizationRequestInterceptor(string.Empty, string.Empty, string.Empty, string.Empty));

                this.runner.RunRestApiPipeline(container, job, new CancellationToken());
            }
            catch (Exception e)
            {
                LoggingHelper2.Debug($"Exception thrown by Databus: {e}");
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
            Binding[] allBindings,
            Entity destinationEntity,
            List<DataSource> dataSources,
            ObjectReference relationshipToParent,
            string path,
            bool isFirst)
        {
            var sourceEntity = await this.GetEntityFromBinding(rootBinding);
            LoggingHelper2.Debug($"GenerateDataSources -- sourceEntity: {JsonConvert.SerializeObject(sourceEntity)}");
            if (isFirst)
            {
                dataSources.Add(
                    new TopLevelDataSource
                        {
                            Path = path,
                            Key = sourceEntity.Fields.First(field => field.IsPrimaryKey).FieldName,
                            TableOrView = this.GetFullyQualifiedTableName(sourceEntity),
                            MySqlEntityColumnMappings =
                                await this.GetColumnsFromEntity(sourceEntity, destinationEntity, rootBinding.SourcedByEntities.First().SourceAliasName),
                            PropertyType = null,
                            MyRelationships = new List<SqlRelationship>()
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

            var childObjectRelationships = this.GetChildObjectRelationships(rootBinding);
            var hasChildren = childObjectRelationships.Count > 0;
            if (!hasChildren)
            {
                return;
            }
            
            foreach (var childObjectRelationship in childObjectRelationships)
            {
                if (childObjectRelationship != null)
                {
                    var childBinding = this.GetMatchingChild(allBindings, childObjectRelationship.ChildObjectId);
                    await this.GenerateDataSources(
                        childBinding,
                        allBindings,
                        destinationEntity,
                        dataSources,
                        childObjectRelationship,
                        string.Join(".", path, await this.GetSourceAliasName(childBinding)),
                        isFirst: false);
                }
            }
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

                LoggingHelper2.Debug($"SourceEntity: {JsonConvert.SerializeObject(sourceEntity)}");
                LoggingHelper2.Debug($"ancestorEntity: {JsonConvert.SerializeObject(ancestorEntity)}");

                sqlRelationships.Add(
                    new SqlRelationship
                        {
                            MySource = new SqlRelationshipEntity
                                           {
                                               Entity = this.GetFullyQualifiedTableName(ancestorEntity),
                                               Key = this.CleanJson(
                                                   ancestorRelationship.AttributeValues.GetAttributeTextValue(AttributeName.ParentKeyFields))
                                           }, // TODO - databus doesn't currently handle comma separated lists here
                            MyDestination =
                                new SqlRelationshipEntity
                                    {
                                        Entity = this.GetFullyQualifiedTableName(sourceEntity),
                                        Key = this.CleanJson(ancestorRelationship.AttributeValues.GetAttributeTextValue(AttributeName.ChildKeyFields))
                                    } // TODO - databus doesn't currently handle comma separated lists here
                        });
            }

            return sqlRelationships;
        }

        private async Task<Binding[]> GetBindingsForEntityAsync(Entity entity)
        {
            var bindingsForDataMart = await this.metadataServiceClient.GetBindingsForDataMartAsync(entity.DataMartId);

            return bindingsForDataMart.Where(binding => binding.DestinationEntityId == entity.Id).ToArray();
        }

        private string GetCardinalityFromObjectReference(ObjectReference objectReference)
        {
            LoggingHelper2.Debug("Entering GetCardinalityFromObjectReference(...)");
            LoggingHelper2.Debug($"objectReference: {JsonConvert.SerializeObject(objectReference)}");
            return this.GetAttributeValueFromObjectReference(objectReference, AttributeName.Cardinality).Equals("array", StringComparison.CurrentCultureIgnoreCase) ? "array" : "object";
        }

        private string GetAttributeValueFromObjectReference(ObjectReference objectReference, string attributeName)
        {
            LoggingHelper2.Debug("Entering GetAttributeValueFromObjectReference(...)");
            LoggingHelper2.Debug($"objectReference: {JsonConvert.SerializeObject(objectReference)}");
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
            var childRelationships = binding.ObjectRelationships.Where(
                    or => or.ChildObjectType == MetadataObjectType.Binding
                          && or.AttributeValues.First(attr => attr.AttributeName == AttributeName.GenerationGap).ValueToInt()
                          == 1)
                .ToList();

            LoggingHelper2.Debug($"Found the following childRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(childRelationships)}");
            return childRelationships;
        }

        private List<BindingReference> GetAncestorObjectRelationships(Binding binding, Binding[] allBindings)
        {
            LoggingHelper2.Debug("Entering GetAncestorObjectRelationships(...)");
            var parentRelationships = new List<BindingReference>();
            foreach (var otherBinding in allBindings.Where(b => b.Id != binding.Id))
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

            LoggingHelper2.Debug($"Found the following parentRelationships for binding with id = {binding.Id}: \n{JsonConvert.SerializeObject(parentRelationships)}");

            return parentRelationships;
        }

        private async Task<Entity> GetEntityFromBinding(Binding binding)
        {
            LoggingHelper2.Debug("Entering GetEntityFromBinding(...)");
            LoggingHelper2.Debug("binding: " + JsonConvert.SerializeObject(binding));

            if (binding == null || !binding.SourcedByEntities.Any() || binding.SourcedByEntities.FirstOrDefault() == null)
            {
                return null;
            }

            var entityReference = binding.SourcedByEntities.First();
            var entity = await this.metadataServiceClient.GetEntityAsync(entityReference.SourceEntityId);
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
    }
}
