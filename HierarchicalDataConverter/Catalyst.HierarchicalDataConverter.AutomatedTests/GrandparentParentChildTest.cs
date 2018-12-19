namespace Catalyst.HierarchicalDataConverter.AutomatedTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.DataProcessing;
    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;
    using Catalyst.DataProcessing.Shared.Utilities.Context;
    using Catalyst.DataProcessing.Shared.Utilities.Logging;

    using DataConverter;

    using Fabric.Databus.Config;

    using log4net;

    using Microsoft.VisualStudio.TestTools.UnitTesting;

    using Moq;

    [TestClass]
    public class GrandparentParentChildTest
    {
        private const int DataMartId = 123;

        private const int SourceConnectionId = 5342;

        [TestMethod]
        public void GrandparentParentChild()
        {
            Binding[] bindings = new Binding[]
                                     {
                                         this.GetNestedBindingLevel0Source(),
                                         this.GetNestedBindingLevel1Source(),
                                         this.GetNestedBindingLevel2Source()
                                     };
            Field[] entity0Fields = this.GetEntity0Fields();
            Field[] entity1Fields = this.GetEntity1Fields();
            Field[] entity2Fields = this.GetEntity2Fields();

            var serviceClientMock = new Mock<IMetadataServiceClient>();
            serviceClientMock.Setup(mock => mock.GetBindingsForDataMartAsync(It.IsAny<int>())).Returns(Task.FromResult(bindings));

            serviceClientMock.Setup(mock => mock.GetEntityFieldsAsync(It.Is<Entity>(entity => entity.Id == 0))).Returns(Task.FromResult(entity0Fields));
            serviceClientMock.Setup(mock => mock.GetEntityFieldsAsync(It.Is<Entity>(entity => entity.Id == 1))).Returns(Task.FromResult(entity1Fields));
            serviceClientMock.Setup(mock => mock.GetEntityFieldsAsync(It.Is<Entity>(entity => entity.Id == 2))).Returns(Task.FromResult(entity2Fields));

            serviceClientMock.Setup(mock => mock.GetEntityAsync(0)).Returns(Task.FromResult(this.GetLevel0SourceEntity()));
            serviceClientMock.Setup(mock => mock.GetEntityAsync(1)).Returns(Task.FromResult(this.GetLevel1SourceEntity()));
            serviceClientMock.Setup(mock => mock.GetEntityAsync(2)).Returns(Task.FromResult(this.GetLevel2SourceEntity()));

            var processingContextWrapperFactoryMock = new Mock<IProcessingContextWrapperFactory>();
            var processingContextWrapperMock = new Mock<IProcessingContextWrapper>();
            processingContextWrapperFactoryMock.Setup(mock => mock.CreateProcessingContextWrapper()).Returns(processingContextWrapperMock.Object);
            processingContextWrapperMock.Setup(mock => mock.GetIncrementalValue(It.IsAny<IncrementalConfiguration>())).Returns(new IncrementalValue { LastMaxIncrementalDate = DateTime.Now });

            var loggingRepositoryMock = new Mock<ILoggingRepository>();

            var converter = new HierarchicalDataTransformer(serviceClientMock.Object, processingContextWrapperFactoryMock.Object, loggingRepositoryMock.Object);
            var privateMethodRunner = new PrivateObject(converter);
            object[] args = new object[] { this.GetNestedBindingLevel0Source(), new BindingExecution(), this.GetNestedDestinationEntity() };

            var jobData = ((Task<JobData>)privateMethodRunner.Invoke("GetJobData", args)).Result;

            Assert.AreEqual(3, jobData.DataSources.Count());
            var firstSource = (TopLevelDataSource)jobData.TopLevelDataSource;
            Assert.AreEqual("[MyDatabaseName].[Level0TableName].[Level0Entity]", firstSource.TableOrView);
            Assert.AreEqual(2, firstSource.SqlEntityColumnMappings.Count());
            Assert.AreEqual("id0", firstSource.SqlEntityColumnMappings.First().Name);
            Assert.AreEqual("Level0FieldToBeAdded", firstSource.SqlEntityColumnMappings.Last().Name);
            Assert.AreEqual("$", firstSource.Path);

            var secondSource = (DataSource)jobData.DataSources.ElementAt(1);
            Assert.AreEqual("[MyDatabaseName].[Level1TableName].[Level1Entity]", secondSource.TableOrView);
            Assert.AreEqual(3, secondSource.SqlEntityColumnMappings.Count());
            Assert.AreEqual("id1", secondSource.SqlEntityColumnMappings.First().Name);
            Assert.AreEqual("id0", secondSource.SqlEntityColumnMappings.ElementAt(1).Name);
            Assert.AreEqual("Level1FieldToBeAdded", secondSource.SqlEntityColumnMappings.Last().Name);
            Assert.AreEqual("$.Level1Entity", secondSource.Path);

            var thirdSource = (DataSource)jobData.DataSources.ElementAt(2);
            Assert.AreEqual("[MyDatabaseName].[Level2TableName].[Level2Entity]", thirdSource.TableOrView);
            Assert.AreEqual(4, thirdSource.SqlEntityColumnMappings.Count());
            Assert.AreEqual("id2", thirdSource.SqlEntityColumnMappings.First().Name);
            Assert.AreEqual("id0", thirdSource.SqlEntityColumnMappings.ElementAt(1).Name);
            Assert.AreEqual("id1", thirdSource.SqlEntityColumnMappings.ElementAt(2).Name);
            Assert.AreEqual("Level2FieldToBeAdded", thirdSource.SqlEntityColumnMappings.Last().Name);
            Assert.AreEqual("$.Level1Entity.Level2Entity", thirdSource.Path);

            Assert.AreEqual(2, thirdSource.Relationships.Count());
            Assert.AreEqual("[MyDatabaseName].[Level2TableName].[Level2Entity]", thirdSource.Relationships.First().Destination.Entity);
            Assert.AreEqual("'id0'", thirdSource.Relationships.First().Destination.Key);
            Assert.AreEqual("[MyDatabaseName].[Level0TableName].[Level0Entity]", thirdSource.Relationships.First().Source.Entity);
            Assert.AreEqual("'id0'", thirdSource.Relationships.First().Source.Key);
            Assert.AreEqual("[MyDatabaseName].[Level2TableName].[Level2Entity]", thirdSource.Relationships.Last().Destination.Entity);
            Assert.AreEqual("'id1'", thirdSource.Relationships.Last().Destination.Key);
            Assert.AreEqual("[MyDatabaseName].[Level1TableName].[Level1Entity]", thirdSource.Relationships.Last().Source.Entity);
            Assert.AreEqual("'id1'", thirdSource.Relationships.Last().Source.Key);

            Assert.AreEqual(1, secondSource.Relationships.Count());
            Assert.AreEqual("[MyDatabaseName].[Level1TableName].[Level1Entity]", secondSource.Relationships.First().Destination.Entity);
            Assert.AreEqual("'id0'", secondSource.Relationships.First().Destination.Key);
            Assert.AreEqual("[MyDatabaseName].[Level0TableName].[Level0Entity]", secondSource.Relationships.First().Source.Entity);
            Assert.AreEqual("'id0'", secondSource.Relationships.First().Destination.Key);

            Assert.AreEqual(0, firstSource.Relationships.Count());
        }

        private Field[] GetEntity0Fields()
        {
            return new[]
                       {
                           new Field
                               {
                                   FieldName = "id0",
                                   Status = FieldStatus.Active,
                                   IsPrimaryKey = true
                               },
                           new Field
                               {
                                   FieldName = "Level0OmittedField",
                                   Status = FieldStatus.Omitted
                               },
                           new Field
                               {
                                   FieldName = "Level0FieldToBeAdded",
                                   Status = FieldStatus.Active
                               }
                       };
        }

        private Field[] GetEntity1Fields()
        {
            return new[]
                       {
                           new Field
                               {
                                   FieldName = "id1",
                                   Status = FieldStatus.Active,
                                   IsPrimaryKey = true
                               },
                           new Field
                               {
                                   FieldName = "id0",
                                   Status = FieldStatus.Active
                               },
                           new Field
                               {
                                   FieldName = "Level1OmittedField",
                                   Status = FieldStatus.Omitted
                               },
                           new Field
                               {
                                   FieldName = "Level1FieldToBeAdded",
                                   Status = FieldStatus.Active
                               }
                       };
        }

        private Field[] GetEntity2Fields()
        {
            return new[]
                       {
                           new Field
                               {
                                   FieldName = "id2",
                                   Status = FieldStatus.Active,
                                   IsPrimaryKey = true
                               },
                           new Field
                               {
                                   FieldName = "id0",
                                   Status = FieldStatus.Active
                               },
                           new Field
                               {
                                   FieldName = "id1",
                                   Status = FieldStatus.Active
                               },
                           new Field
                               {
                                   FieldName = "Level2OmittedField",
                                   Status = FieldStatus.Omitted
                               },
                           new Field
                               {
                                   FieldName = "Level2FieldToBeAdded",
                                   Status = FieldStatus.Active
                               }
                       };
        }

        private Binding GetNestedBindingLevel0Source()
        {
            var destinationEntity = this.GetNestedDestinationEntity();
            var nestedBindingLevel1 = this.GetNestedBindingLevel1Source();
            var nestedBindingLevel2 = this.GetNestedBindingLevel2Source();
            var level0SourceEntity = this.GetLevel0SourceEntity();
            return new Binding
                       {
                           DestinationEntityId = destinationEntity.Id,
                           ContentId = Guid.NewGuid(),
                           DataMartId = DataMartId,
                           Classification = "Generic",
                           Name = "NestedBindingLevel0Source",
                           SourceConnectionId = SourceConnectionId,
                           BindingType = "Nested",
                           Status = "Active",
                           LoadTypeCode = "Full",
                           Id = 0,
                           SourcedByEntities = { new SourceEntityReference { SourceEntityId = level0SourceEntity.Id } },
                           ObjectRelationships =
                               {
                                   new ObjectReference
                                       {
                                           ChildObjectId =
                                               nestedBindingLevel1.Id,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new
                                                   ObjectAttributeValue
                                                       {
                                                           AttributeName
                                                               = "Cardinality",
                                                           AttributeValue
                                                               = "SingleObject"
                                                       },
                                                   new
                                                   ObjectAttributeValue
                                                       {
                                                           AttributeName
                                                               = "ParentKeyFields",
                                                           AttributeValue
                                                               = "['id0']"
                                                       },
                                                   new
                                                   ObjectAttributeValue
                                                       {
                                                           AttributeName
                                                               = "ChildKeyFields",
                                                           AttributeValue
                                                               = "['id0']"
                                                       },
                                                   new ObjectAttributeValue
                                                       {
                                                           AttributeName = "GenerationGap",
                                                           AttributeValue = "1"
                                                       }
                                               }
                                       },
                                   new ObjectReference
                                       {
                                           ChildObjectId =
                                               nestedBindingLevel2.Id,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new
                                                   ObjectAttributeValue
                                                       {
                                                           AttributeName
                                                               = "Cardinality",
                                                           AttributeValue
                                                               = "SingleObject"
                                                       },
                                                   new
                                                   ObjectAttributeValue
                                                       {
                                                           AttributeName
                                                               = "ParentKeyFields",
                                                           AttributeValue
                                                               = "['id0']"
                                                       },
                                                   new
                                                   ObjectAttributeValue
                                                       {
                                                           AttributeName
                                                               = "ChildKeyFields",
                                                           AttributeValue
                                                               = "['id0']"
                                                       },
                                                   new ObjectAttributeValue
                                                       {
                                                           AttributeName = "GenerationGap",
                                                           AttributeValue = "2"
                                                       }
                                               }
                                       }
                               },
                       };
        }

        private Binding GetNestedBindingLevel1Source()
        {
            var destinationEntity = this.GetNestedDestinationEntity();
            var nestedBindingLevel2 = this.GetNestedBindingLevel2Source();
            var level1SourceEntity = this.GetLevel1SourceEntity();
            return new Binding
                       {
                           DestinationEntityId = destinationEntity.Id,
                           ContentId = Guid.NewGuid(),
                           DataMartId = DataMartId,
                           Classification = "Generic",
                           Name = "NestedBindingLevel1Source",
                           SourceConnectionId = SourceConnectionId,
                           BindingType = "Nested",
                           Status = "Active",
                           LoadTypeCode = "Full",
                           Id = 1,
                           SourcedByEntities =
                               {
                                   new SourceEntityReference
                                       {
                                           SourceEntityId = level1SourceEntity.Id
                                       }
                               },
                           ObjectRelationships =
                               {
                                   new ObjectReference
                                       {
                                           ChildObjectId = nestedBindingLevel2.Id,
                                           ChildObjectType = "Binding",
                                           AttributeValues =
                                               {
                                                   new ObjectAttributeValue
                                                       {
                                                           AttributeName = "Cardinality",
                                                           AttributeValue = "SingleObject"
                                                       },
                                                   new ObjectAttributeValue
                                                       {
                                                           AttributeName = "ParentKeyFields",
                                                           AttributeValue = "['id1']"
                                                       },
                                                   new ObjectAttributeValue
                                                       {
                                                           AttributeName = "ChildKeyFields",
                                                           AttributeValue = "['id1']"
                                                       },
                                                   new ObjectAttributeValue
                                                       {
                                                           AttributeName = "GenerationGap",
                                                           AttributeValue = "1"
                                                       }
                                               }
                                       }
                               }
            };
        }

        private Binding GetNestedBindingLevel2Source()
        {
            var destinationEntity = this.GetNestedDestinationEntity();
            var level2SourceEntity = this.GetLevel2SourceEntity();
            return new Binding
                       {
                           DestinationEntityId = destinationEntity.Id,
                           ContentId = Guid.NewGuid(),
                           DataMartId = DataMartId,
                           Classification = "Generic",
                           Name = "NestedBindingLevel2Source",
                           SourceConnectionId = SourceConnectionId,
                           BindingType = "Nested",
                           Status = "Active",
                           LoadTypeCode = "Full",
                           Id = 2,
                           SourcedByEntities =
                               {
                                   new SourceEntityReference
                                       {
                                           SourceEntityId = level2SourceEntity.Id
                                       }
                               }
            };
        }

        private Entity GetNestedDestinationEntity()
        {
            return new Entity
                       {
                           Id = 6357,
                           DataMartId = DataMartId,
                           Fields =
                               {
                                   new Field
                                       {
                                           FieldName = "Level0Entity__id0",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level0Entity__Level0EntityPrimaryKey",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level0Entity__Level0OmittedField",
                                           Status = FieldStatus.Omitted
                                       },
                                   new Field
                                       {
                                           FieldName = "Level0Entity__Level0FieldToBeAdded",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity__id0",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity__id1",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity__Level1EntityPrimaryKey",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity__Level1OmittedField",
                                           Status = FieldStatus.Omitted
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity__Level1FieldToBeAdded",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity__id0",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity__id1",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity__id2",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity__Level2EntityPrimaryKey",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity__Level2OmittedField",
                                           Status = FieldStatus.Omitted
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity__Level2FieldToBeAdded",
                                           Status = FieldStatus.Active
                                       }
                               }
                       };
        }

        private Entity GetLevel0SourceEntity()
        {
            var entity = new Entity
                       {
                           Id = 0,
                           EntityName = "Level0Entity",
                           DataMartId = DataMartId,
                           DatabaseName = "MyDatabaseName",
                           SchemaName = "Level0TableName"
                       };
            var fields = this.GetEntity0Fields();
            foreach (var field in fields)
            {
                entity.Fields.Add(field);
            }

            return entity;
        }

        private Entity GetLevel1SourceEntity()
        {
            var entity = new Entity
                       {
                           Id = 1,
                           EntityName = "Level1Entity",
                           DataMartId = DataMartId,
                           DatabaseName = "MyDatabaseName",
                           SchemaName = "Level1TableName"
            };
            var fields = this.GetEntity1Fields();
            foreach (var field in fields)
            {
                entity.Fields.Add(field);
            }

            return entity;
        }

        private Entity GetLevel2SourceEntity()
        {
            var entity = new Entity
                       {
                           Id = 2,
                           EntityName = "Level2Entity",
                           DataMartId = DataMartId,
                           DatabaseName = "MyDatabaseName",
                           SchemaName = "Level2TableName"
            };
            var fields = this.GetEntity2Fields();
            foreach (var field in fields)
            {
                entity.Fields.Add(field);
            }

            return entity;
        }
    }
}
