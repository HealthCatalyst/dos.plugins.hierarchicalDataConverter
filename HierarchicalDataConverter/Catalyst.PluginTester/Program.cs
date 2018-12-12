namespace Catalyst.PluginTester
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using Unity;
    using Unity.Interception.Utilities;

    public class Program
    {
        public static void Main(string[] args)
        {
            try
            {
                var bindingExecution = new BindingExecution();
                // ReSharper disable once StringLiteralTypo
                const string DatabaseName = "SAMHCOS";
                // ReSharper disable once StringLiteralTypo
                const string SchemaName = "HCOSText";
                const int DestinationEntityId = 1;
                const int DataEntityId = 2;
                const int PatientEntityId = 3;
                const int VisitEntityId = 4;
                const int VisitFacilityEntityId = 5;

                const string ChildObjectType = "Binding";
                const string BindingType = "Nested";

                const string DestinationEntityName = "3GenNestedDestinationEntity";
                const string DataEntityName = "Data";
                const string PatientEntityName = "Patient";
                const string VisitEntityName = "Visit";
                const string VisitFacilityEntityName = "VisitFacility";

                const int NestedDataBindingId = 102;
                const int NestedPatientBindingId = 103;
                const int NestedVisitBindingId = 104;
                const int NestedVisitFacilityBindingId = 105;

                const string AttributeGenerationGap = "GenerationGap";
                const string AttributeParentKeyFields = "ParentKeyFields";
                const string AttributeChildKeyFields = "ChildKeyFields";
                const string AttributeCardinality = "Cardinality";
                const string AttributeValueSingleObject = "SingleObject";

                const string PatientKey = "PatientKEY";
                const string VisitKey = "VisitKEY";
                const string VisitFacilityKey = "VisitFacilityKEY";

                var dataMart = new DataMart();
                dataMart.Entities.Add(new Entity
                {
                    Id = DestinationEntityId,
                    EntityName = DestinationEntityName,
                    DatabaseName = DatabaseName,
                    SchemaName = SchemaName
                });
                dataMart.Entities.Add(new Entity
                {
                    Id = DataEntityId,
                    EntityName = DataEntityName,
                    DatabaseName = DatabaseName,
                    SchemaName = SchemaName
                });
                dataMart.Entities.Add(new Entity
                {
                    Id = PatientEntityId,
                    EntityName = PatientEntityName,
                    DatabaseName = DatabaseName,
                    SchemaName = SchemaName
                });
                dataMart.Entities.Add(new Entity
                {
                    Id = VisitEntityId,
                    EntityName = VisitEntityName,
                    DatabaseName = DatabaseName,
                    SchemaName = SchemaName
                });
                dataMart.Entities.Add(new Entity
                {
                    Id = VisitFacilityEntityId,
                    EntityName = VisitFacilityEntityName,
                    DatabaseName = DatabaseName,
                    SchemaName = SchemaName
                });

                var dataEntity = dataMart.Entities.First(entity => entity.Id == DataEntityId);
                dataEntity.Fields.Add(new Field { IsPrimaryKey = true, FieldName = "TextKEY" });
                dataEntity.Fields.Add(new Field { FieldName = "root" });
                dataEntity.Fields.Add(new Field { FieldName = "extension" });
                dataEntity.Fields.Add(new Field { FieldName = "extension_suffix" });
                dataEntity.Fields.Add(new Field { FieldName = "data" });
                dataEntity.Fields.Add(new Field { FieldName = "base64_data" });
                dataEntity.Fields.Add(new Field { FieldName = "data_format" });
                dataEntity.Fields.Add(new Field { FieldName = "source_last_modified_at" });
                dataEntity.Fields.Add(new Field { FieldName = "source_versioned_at" });

                var patientEntity = dataMart.Entities.First(entity => entity.Id == PatientEntityId);
                patientEntity.Fields.Add(new Field { FieldName = "extension" });
                patientEntity.Fields.Add(new Field { FieldName = "root" });
                patientEntity.Fields.Add(new Field { FieldName = "last_name" });
                patientEntity.Fields.Add(new Field { FieldName = "first_name" });
                patientEntity.Fields.Add(new Field { FieldName = "middle_name" });
                patientEntity.Fields.Add(new Field { FieldName = "gender" });
                patientEntity.Fields.Add(new Field { FieldName = "date_of_birth" });

                var visitEntity = dataMart.Entities.First(entity => entity.Id == VisitEntityId);
                visitEntity.Fields.Add(new Field { FieldName = "extension" });
                visitEntity.Fields.Add(new Field { FieldName = "root" });
                visitEntity.Fields.Add(new Field { FieldName = "admitted_at" });
                visitEntity.Fields.Add(new Field { FieldName = "discharged_at" });

                var visitFacilityEntity = dataMart.Entities.First(entity => entity.Id == VisitFacilityEntityId);
                visitFacilityEntity.Fields.Add(new Field { FieldName = "extension" });
                visitFacilityEntity.Fields.Add(new Field { FieldName = "root" });


                var destinationEntity = dataMart.Entities.First(entity => entity.Id == DestinationEntityId);
                destinationEntity.Fields.Add(new Field { IsPrimaryKey = true, FieldName = "TextKEY" });
                dataEntity.Fields.ForEach(
                    field => destinationEntity.Fields.Add(
                        new Field { FieldName = $"{DataEntityName}_{field.FieldName}" }));
                patientEntity.Fields.ForEach(
                    field => destinationEntity.Fields.Add(
                        new Field { FieldName = $"{PatientEntityName}_{field.FieldName}" }));
                visitEntity.Fields.ForEach(
                    field => destinationEntity.Fields.Add(
                        new Field { FieldName = $"{VisitEntityName}_{field.FieldName}" }));

                dataMart.Bindings.Add(new Binding
                {
                    Id = NestedDataBindingId,
                    DestinationEntityId = DestinationEntityId,
                    ContentId = Guid.NewGuid(),
                    Classification = "Generic",
                    Name = "NestedBinding" + DataEntityName,
                    BindingType = BindingType,
                    Status = "Active",
                    LoadTypeCode = "Full",
                    SourcedByEntities = { new SourceEntityReference { SourceEntityId = DataEntityId } }
                });
                dataMart.Bindings.Add(new Binding
                {
                    Id = NestedPatientBindingId,
                    DestinationEntityId = DestinationEntityId,
                    ContentId = Guid.NewGuid(),
                    Classification = "Generic",
                    Name = "NestedBinding" + PatientEntityName,
                    BindingType = BindingType,
                    Status = "Active",
                    LoadTypeCode = "Full",
                    SourcedByEntities = { new SourceEntityReference { SourceEntityId = PatientEntityId } }
                });
                dataMart.Bindings.Add(new Binding
                {
                    Id = NestedVisitBindingId,
                    DestinationEntityId = DestinationEntityId,
                    ContentId = Guid.NewGuid(),
                    Classification = "Generic",
                    Name = "NestedBinding" + VisitEntityName,
                    BindingType = BindingType,
                    Status = "Active",
                    LoadTypeCode = "Full",
                    SourcedByEntities = { new SourceEntityReference { SourceEntityId = VisitEntityId } }
                });
                dataMart.Bindings.Add(new Binding
                {
                    Id = NestedVisitFacilityBindingId,
                    DestinationEntityId = DestinationEntityId,
                    ContentId = Guid.NewGuid(),
                    Classification = "Generic",
                    Name = "NestedBinding" + VisitFacilityEntityName,
                    BindingType = BindingType,
                    Status = "Active",
                    LoadTypeCode = "Full",
                    SourcedByEntities = { new SourceEntityReference { SourceEntityId = VisitFacilityEntityId } }
                });

                // relate Data to Patient on PatientKEY
                dataMart.Bindings.First(binding => binding.Id == NestedDataBindingId).ObjectRelationships.Add(
                    new ObjectReference
                    {
                        ChildObjectType = ChildObjectType,
                        ChildObjectId = NestedPatientBindingId,
                        AttributeValues = new List<ObjectAttributeValue>
                                                  {
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeGenerationGap, AttributeValue = "1"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeParentKeyFields,
                                                              AttributeValue = $"[\"{PatientKey}\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeChildKeyFields,
                                                              AttributeValue = $"[\"{PatientKey}\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeCardinality,
                                                              AttributeValue = AttributeValueSingleObject
                                                          }
                                                  }
                    });

                // relate Data to Visit on VisitKEY
                dataMart.Bindings.First(binding => binding.Id == NestedDataBindingId).ObjectRelationships.Add(
                    new ObjectReference
                    {
                        ChildObjectType = ChildObjectType,
                        ChildObjectId = NestedVisitBindingId,
                        AttributeValues = new List<ObjectAttributeValue>
                                                  {
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeGenerationGap, AttributeValue = "1"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeParentKeyFields,
                                                              AttributeValue = $"[\"{VisitKey}\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeChildKeyFields,
                                                              AttributeValue = $"[\"{VisitKey}\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeCardinality,
                                                              AttributeValue = AttributeValueSingleObject
                                                          }
                                                  }
                    });

                dataMart.Bindings.First(binding => binding.Id == NestedDataBindingId).ObjectRelationships.Add(
                    new ObjectReference
                    {
                        ChildObjectType = ChildObjectType,
                        ChildObjectId = NestedVisitFacilityBindingId,
                        AttributeValues = new List<ObjectAttributeValue>
                                                  {
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeGenerationGap, AttributeValue = "2"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeParentKeyFields,
                                                              AttributeValue = $"[\"{VisitFacilityKey}\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeChildKeyFields,
                                                              AttributeValue = $"[\"{VisitFacilityKey}\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = AttributeCardinality,
                                                              AttributeValue = AttributeValueSingleObject
                                                          }
                                                  }
                    });

                var unityContainer = new UnityContainer();

                var testMetadataServiceClient = new TestMetadataServiceClient();
                testMetadataServiceClient.Init(dataMart);
                unityContainer.RegisterInstance<IMetadataServiceClient>(testMetadataServiceClient);

                var pluginLoader = new PluginLoader();
                pluginLoader.LoadPlugins();
                pluginLoader.RegisterPlugins(unityContainer);

                var hierarchicalDataTransformer = pluginLoader.GetPluginOfExactType<IDataTransformer>("HierarchicalDataTransformer");

                var transformDataAsync = hierarchicalDataTransformer.TransformDataAsync(
                    bindingExecution,
                    dataMart.Bindings.First(),
                    dataMart.Entities.First(),
                    CancellationToken.None)
                    .Result;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.ReadKey();
            }
        }
    }
}
