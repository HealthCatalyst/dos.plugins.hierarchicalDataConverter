namespace Catalyst.PluginTester
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Reflection;

    using Catalyst.DataProcessing.Engine.PluginInterfaces;
    using Catalyst.Platform.CommonExtensions;
    using Catalyst.Platform.CommonExtensions.ExceptionExtensions;

    using Unity;
    using Unity.Injection;
    using Unity.Interception.Utilities;
    using Unity.RegistrationByConvention;

    public class PluginLoader
    {
        private const string PluginsSectionGroupName = "plugins";
        private const string SourceSystemsSectionName = "sourceSystems";
        private const string DataTransformersSectionName = "dataTransformers";
        private const string DestinationSystemsSectionName = "destinationSystems";
        // ReSharper disable once IdentifierTypo
        private const string MigratorsSectionName = "migrators";
        // ReSharper disable once IdentifierTypo
        private const string QueryRewritersSectionName = "queryRewriters";
        private const string FileSystemsSectionName = "fileSystems";

        private readonly Dictionary<Type, object> plugins = new Dictionary<Type, object>();

        public void LoadPlugins()
        {
            // load plugin assemblies
            string pluginsFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Plugins");

            if (!Directory.Exists(pluginsFolder))
            {
                return;
            }

            DirectoryInfo pluginDirectory = new DirectoryInfo(pluginsFolder);
            foreach (FileInfo assemblyFile in pluginDirectory.GetFiles("*.dll"))
            {
                try
                {
                    AssemblyName assemblyName = AssemblyName.GetAssemblyName(assemblyFile.FullName);
                    AppDomain.CurrentDomain.Load(assemblyName);
                }
                catch (FileLoadException exception)
                {
                    throw new InvalidOperationException(
                        Properties.Resources.FailedToLoadAssemblyFile.FormatCurrentCulture(assemblyFile.FullName, exception.ToDetailedString()),
                        exception);
                }
                catch (BadImageFormatException exception)
                {
                    throw new InvalidOperationException(
                        Properties.Resources.FailedToLoadAssemblyFile.FormatCurrentCulture(assemblyFile.FullName, exception.ToDetailedString()),
                        exception);
                }
            }
       }

        public void RegisterPlugins(IUnityContainer container)
        {
            this.AddPlugins<ISourceSystem>(SourceSystemsSectionName, container);
            this.AddPlugins<IDataTransformer>(DataTransformersSectionName, container);
            this.AddPlugins<IDestinationSystem>(DestinationSystemsSectionName, container);
            this.AddPlugins<IMigrator>(MigratorsSectionName, container);
            this.AddPlugins<IQueryRewriter>(QueryRewritersSectionName, container);
            this.AddPlugins<IFileSystem>(FileSystemsSectionName, container);
        }

        public List<T> GetPlugins<T>()
        {
            return this.plugins.Where(plugin => plugin.Key == typeof(T)).Select(plugin => plugin.Value).OfType<T>().ToList();
        }

        public T GetPluginOfExactType<T>(string name)
        {
            return this.plugins.Where(plugin => plugin.Key == typeof(T)).Select(plugin => plugin.Value).OfType<T>().First(plugin => plugin.GetType().Name.Equals(name));
        }

        private void AddPlugins<T>(string sectionName, IUnityContainer container)
            where T : class
        {
            var implementations = this.RegisterImplementations<T>(PluginsSectionGroupName, sectionName, container);
            implementations.ForEach(implementation => this.plugins.Add(typeof(T), implementation));
        }

        private IEnumerable<T> RegisterImplementations<T>(string sectionGroupName, string sectionName, IUnityContainer container) where T : class
        {
            container.RegisterTypes(
                AllClasses.FromLoadedAssemblies().Where(type => typeof(T).IsAssignableFrom(type)),
                WithMappings.FromAllInterfaces,
                WithName.TypeName,
                WithLifetime.Transient);

            IList<T> allImplementations = container.ResolveAll<T>().ToList();
            NameValueCollection collection =
                (NameValueCollection)ConfigurationManager.GetSection("{0}/{1}".FormatInvariantCulture(sectionGroupName, sectionName));
            IList<string> specifiedImplementations = collection.AllKeys;

            IList<T> result = new List<T>();
            foreach (string implementationName in specifiedImplementations)
            {
                T implementation = allImplementations.Single(i => i.GetType().FullName == implementationName);
                result.Add(implementation);
            }

            foreach (T implementation in allImplementations.Where(x => result.All(y => x.GetType() != y.GetType())))
            {
                result.Add(implementation);
            }

            return result;
        }
    }
}
