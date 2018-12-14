using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Catalyst.HierarchicalDataConverter.AutomatedTests
{
    using System.Linq;
    using System.Threading.Tasks;

    using Catalyst.DataProcessing.Shared.Models.Enums;
    using Catalyst.DataProcessing.Shared.Models.Metadata;
    using Catalyst.DataProcessing.Shared.Utilities.Client;

    using DataConverter;

    using Fabric.Databus.Config;

    using Moq;

    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            Binding[] bindings = new Binding[3]
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

            var converter = new HierarchicalDataTransformer(serviceClientMock.Object);
            var privateMethodRunner = new PrivateObject(converter);
            object[] args = new object[2] { this.GetNestedBindingLevel0Source(), this.GetNestedDestinationEntity() };

            var jobData = ((Task<JobData>)privateMethodRunner.Invoke("GetJobData", args)).Result;

            Assert.AreEqual(3, jobData.DataSources.Count());
            var firstSource = (TopLevelDataSource)jobData.TopLevelDataSource;
            Assert.AreEqual("[MyDatabaseName].[Level0TableName].[Level0Entity]", firstSource.TableOrView);
            Assert.AreEqual(2, firstSource.SqlEntityColumnMappings.Count());
            Assert.AreEqual("Level0EntityPrimaryKey", firstSource.SqlEntityColumnMappings.First().Name);
            Assert.AreEqual("Level0FieldToBeAdded", firstSource.SqlEntityColumnMappings.Last().Name);
            Assert.AreEqual("$", firstSource.Path);

            var secondSource = (DataSource)jobData.DataSources.ElementAt(1);
            Assert.AreEqual("[MyDatabaseName].[Level1TableName].[Level1Entity]", secondSource.TableOrView);
            Assert.AreEqual(2, secondSource.SqlEntityColumnMappings.Count());
            Assert.AreEqual("Level1EntityPrimaryKey", secondSource.SqlEntityColumnMappings.First().Name);
            Assert.AreEqual("Level1FieldToBeAdded", secondSource.SqlEntityColumnMappings.Last().Name);
            Assert.AreEqual("$.Level1Entity", secondSource.Path);

            var thirdSource = (DataSource)jobData.DataSources.ElementAt(2);
            Assert.AreEqual("[MyDatabaseName].[Level2TableName].[Level2Entity]", thirdSource.TableOrView);
            Assert.AreEqual(2, thirdSource.SqlEntityColumnMappings.Count());
            Assert.AreEqual("Level2EntityPrimaryKey", thirdSource.SqlEntityColumnMappings.First().Name);
            Assert.AreEqual("Level2FieldToBeAdded", thirdSource.SqlEntityColumnMappings.Last().Name);
            Assert.AreEqual("$.Level1Entity.Level2Entity", thirdSource.Path);

        }

        private Field[] GetEntity0Fields()
        {
            return new Field[3]
                       {
                           new Field
                               {
                                   FieldName = "Level0EntityPrimaryKey",
                                   Status = FieldStatus.Active
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
            return new Field[3]
                       {
                           new Field
                               {
                                   FieldName = "Level1EntityPrimaryKey",
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
            return new Field[3]
                       {
                           new Field
                               {
                                   FieldName = "Level2EntityPrimaryKey",
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
                           DataMartId = this.DataMartId,
                           Classification = "Generic",
                           Name = "NestedBindingLevel0Source",
                           SourceConnectionId = this.SourceConnectionId,
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
            var level2SourceEntity = this.GetLevel2SourceEntity();
            return new Binding
                       {
                           DestinationEntityId = destinationEntity.Id,
                           ContentId = Guid.NewGuid(),
                           DataMartId = this.DataMartId,
                           Classification = "Generic",
                           Name = "NestedBindingLevel1Source",
                           SourceConnectionId = this.SourceConnectionId,
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
            var level1SourceEntity = this.GetLevel1SourceEntity();
            var level2SourceEntity = this.GetLevel2SourceEntity();
            return new Binding
                       {
                           DestinationEntityId = destinationEntity.Id,
                           ContentId = Guid.NewGuid(),
                           DataMartId = this.DataMartId,
                           Classification = "Generic",
                           Name = "NestedBindingLevel2Source",
                           SourceConnectionId = this.SourceConnectionId,
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
                           DataMartId = this.DataMartId,
                           Fields =
                               {
                                   new Field
                                       {
                                           FieldName = "Level0Entity_Level0EntityPrimaryKey",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level0Entity_Level0OmittedField",
                                           Status = FieldStatus.Omitted
                                       },
                                   new Field
                                       {
                                           FieldName = "Level0Entity_Level0FieldToBeAdded",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity_Level1EntityPrimaryKey",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity_Level1OmittedField",
                                           Status = FieldStatus.Omitted
                                       },
                                   new Field
                                       {
                                           FieldName = "Level1Entity_Level1FieldToBeAdded",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity_Level2EntityPrimaryKey",
                                           Status = FieldStatus.Active
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity_Level2OmittedField",
                                           Status = FieldStatus.Omitted
                                       },
                                   new Field
                                       {
                                           FieldName = "Level2Entity_Level2FieldToBeAdded",
                                           Status = FieldStatus.Active
                                       }
                               }
                       };
        }

        private Entity GetLevel0SourceEntity()
        {
            return new Entity
                       {
                           Id = 0,
                           EntityName = "Level0Entity",
                           DataMartId = this.DataMartId,
                           Fields = { new Field { IsPrimaryKey = true, FieldName = "Level0EntityPrimaryKey" } },
                           DatabaseName = "MyDatabaseName",
                           SchemaName = "Level0TableName"
                       };
        }

        private Entity GetLevel1SourceEntity()
        {
            return new Entity
                       {
                           Id = 1,
                           EntityName = "Level1Entity",
                           DataMartId = this.DataMartId,
                           Fields = { new Field { IsPrimaryKey = true, FieldName = "Level1EntityPrimaryKey" } },
                           DatabaseName = "MyDatabaseName",
                           SchemaName = "Level1TableName"
            };
        }

        private Entity GetLevel2SourceEntity()
        {
            return new Entity
                       {
                           Id = 2,
                           EntityName = "Level2Entity",
                           DataMartId = this.DataMartId,
                           Fields = { new Field { IsPrimaryKey = true, FieldName = "Level2EntityPrimaryKey" } },
                           DatabaseName = "MyDatabaseName",
                           SchemaName = "Level2TableName"
            };
        }

        private int DataMartId
        {
            get
            {
                return 123;
            }
        }

        private int SourceConnectionId
        {
            get
            {
                return 5342;
            }
        }
    }
}
