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
                const string DatabaseName = "MyDatabase";
                const string SchemaName = "MySchema";
                const int DestinationEntityId = 1;
                const int Level0SourceEntityNewId = 2;
                const int Level1SourceEntityNewId = 3;
                const int Level2SourceEntityNewId = 4;

                const int NestedLevel0BindingId = 102;
                const int NestedLevel1BindingId = 103;
                const int NestedLevel2BindingId = 104;

                var entities = new List<Entity>
                                    {
                                        new Entity
                                            {
                                                Id = DestinationEntityId,
                                                EntityName = "3GenNestedDestinationEntity",
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = Level0SourceEntityNewId,
                                                EntityName = "Level0Entity",
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = Level1SourceEntityNewId,
                                                EntityName = "Level1Entity",
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            },
                                        new Entity
                                            {
                                                Id = Level2SourceEntityNewId,
                                                EntityName = "Level2Entity",
                                                DatabaseName = DatabaseName,
                                                SchemaName = SchemaName
                                            }
                                    };
            
                var bindings = new List<Binding>
                                    {
                                        new Binding
                                            {
                                                Id = NestedLevel0BindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBindingLevel0Source",
                                                BindingType = "Nested",
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = Level0SourceEntityNewId } }
                                            },
                                        new Binding
                                            {
                                                Id = NestedLevel1BindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBindingLevel1Source",
                                                BindingType = "Nested",
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = Level1SourceEntityNewId } }
                                            },
                                        new Binding
                                            {
                                                Id = NestedLevel2BindingId,
                                                DestinationEntityId = DestinationEntityId,
                                                ContentId = Guid.NewGuid(),
                                                Classification = "Generic",
                                                Name = "NestedBindingLevel2Source",
                                                BindingType = "Nested",
                                                Status = "Active",
                                                LoadTypeCode = "Full",
                                                SourcedByEntities = { new SourceEntityReference { SourceEntityId = Level2SourceEntityNewId } }
                                            }
                                    };

                bindings.First(binding => binding.Id == NestedLevel0BindingId).ObjectRelationships.Add(
                    new ObjectReference
                        {
                            ChildObjectType = "Binding",
                            ChildObjectId = NestedLevel1BindingId,
                            AttributeValues = new List<ObjectAttributeValue>
                                                  {
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "GenerationGap", AttributeValue = "1"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "ParentKeyFields",
                                                              AttributeValue = "[\"id0\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "ChildKeyFields",
                                                              AttributeValue = "[\"id0\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "Cardinality",
                                                              AttributeValue = "SingleObject"
                                                          }
                                                  }
                        });
                bindings.First(binding => binding.Id == NestedLevel0BindingId).ObjectRelationships.Add(
                    new ObjectReference
                        {
                            ChildObjectType = "Binding",
                            ChildObjectId = NestedLevel2BindingId,
                            AttributeValues = new List<ObjectAttributeValue>
                                                  {
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "GenerationGap", AttributeValue = "2"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "ParentKeyFields",
                                                              AttributeValue = "[\"id0\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "ChildKeyFields",
                                                              AttributeValue = "[\"id0\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "Cardinality",
                                                              AttributeValue = "SingleObject"
                                                          }
                                                  }
                        });
                bindings.First(binding => binding.Id == NestedLevel1BindingId).ObjectRelationships.Add(
                    new ObjectReference
                        {
                            ChildObjectType = "Binding",
                            ChildObjectId = NestedLevel2BindingId,
                            AttributeValues = new List<ObjectAttributeValue>
                                                  {
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "GenerationGap", AttributeValue = "1"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "ParentKeyFields",
                                                              AttributeValue = "[\"id1\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "ChildKeyFields",
                                                              AttributeValue = "[\"id1\"]"
                                                          },
                                                      new ObjectAttributeValue
                                                          {
                                                              AttributeName = "Cardinality",
                                                              AttributeValue = "SingleObject"
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
                    CancellationToken.None);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.ReadKey();
            }
        }
    }
}
