namespace Catalyst.PluginTester
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Metadata;

    using DataConverter;

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
                const string DataEntity = "Data";
                const string PatientEntity = "Patient";
                const string VisitEntity = "Visit";
                const string VisitFacilityEntity = "VisitFacility";

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

                var entities = new List<Entity>
                                    {
                                        new Entity
                                            {
                                                Id = DestinationEntityId,
                                                EntityName = DestinationEntityName,
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = DataEntityId,
                                                EntityName = DataEntity,
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = PatientEntityId,
                                                EntityName = PatientEntity,
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = VisitEntityId,
                                                EntityName = VisitEntity,
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = VisitFacilityEntityId,
                                                EntityName = VisitFacilityEntity,
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            }
                                    };

                entities.First(entity => entity.Id == DataEntityId).Fields
                    .Add(new Field { IsPrimaryKey = true, FieldName = "TextKEY" });

                var bindings = new List<Binding>
                                    {
                                        new Binding
                                            {
                                                Id = NestedDataBindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBinding" + DataEntity,
                                                BindingType = BindingType,
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = DataEntityId } }
                                            },
                                        new Binding
                                            {
                                                Id = NestedPatientBindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBinding" + PatientEntity,
                                                BindingType = BindingType,
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = PatientEntityId } }
                                            },
                                        new Binding
                                            {
                                                Id = NestedVisitBindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBinding" + VisitEntity,
                                                BindingType = BindingType,
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = VisitEntityId } }
                                            },
                                        new Binding
                                            {
                                                Id = NestedVisitFacilityBindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBinding" + VisitFacilityEntity,
                                                BindingType = BindingType,
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = VisitFacilityEntityId } }
                                            }
                                    };

                // relate Data to Patient on PatientKEY
                bindings.First(binding => binding.Id == NestedDataBindingId).ObjectRelationships.Add(
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
                bindings.First(binding => binding.Id == NestedDataBindingId).ObjectRelationships.Add(
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

                bindings.First(binding => binding.Id == NestedDataBindingId).ObjectRelationships.Add(
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

                var entityFields = new List<Field> { new Field { EntityId = 1 } };

                var testMetadataServiceClient = new TestMetadataServiceClient();
                testMetadataServiceClient.Init(entities, bindings, entityFields);
                var hierarchicalDataTransformer = new HierarchicalDataTransformer(testMetadataServiceClient);

                var transformDataAsync = hierarchicalDataTransformer.TransformDataAsync(
                    bindingExecution,
                    bindings[0],
                    entities[0],
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
