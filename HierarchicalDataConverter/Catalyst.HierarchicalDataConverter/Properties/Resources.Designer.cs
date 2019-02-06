﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace DataConverter.Properties {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "15.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("DataConverter.Properties.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Cannot create the connection string for source entities.  Ensure that either the ConnectionString attribute is set on the binding&apos;s source connection, or that the Server and Database values are present..
        /// </summary>
        internal static string CannotBuildConnectionString {
            get {
                return ResourceManager.GetString("CannotBuildConnectionString", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Cannot construct UpmcSpecificConfigurationuration from configuration file.  Please ensure the appropriate values [name, baseUrl, appId, appSecret, tenantSecret] are in the configuration file and that the appropriate configuration is specified in the destination connection attribute &apos;ClientSpecificConfiguration&apos;.
        /// </summary>
        internal static string CannotCreateUpmcConfiguration {
            get {
                return ResourceManager.GetString("CannotCreateUpmcConfiguration", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Cannot retrieve client specific configuration from destination connection attribute &apos;{0}&apos; key = &apos;{1}&apos;.  Ensure that this key maps to a section of the plugin&apos;s configuration file..
        /// </summary>
        internal static string CannotRetrieveClientSpecificConfiguration {
            get {
                return ResourceManager.GetString("CannotRetrieveClientSpecificConfiguration", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to config.json.
        /// </summary>
        internal static string DefaultConfigFileName {
            get {
                return ResourceManager.GetString("DefaultConfigFileName", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Plugin cannot execute &apos;{0}&apos; requests at this time.  Only &apos;POST&apos; and &apos;PUT&apos; requests are currently supported..
        /// </summary>
        internal static string InvalidHttpMethod {
            get {
                return ResourceManager.GetString("InvalidHttpMethod", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The logging level provided by the either the system attribute [&apos;HierarchicalPluginLogLevel&apos;], or in the plugin configuration [&apos;LogLevel&apos;] was invalid: &apos;{0}&apos;.  Value must be one of Verbose | Debug | Information | Warning | Error | Fatal..
        /// </summary>
        internal static string InvalidLogLevel {
            get {
                return ResourceManager.GetString("InvalidLogLevel", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to server={0};initial catalog={1};Trusted_Connection=True;.
        /// </summary>
        internal static string SqlServerConnectionString {
            get {
                return ResourceManager.GetString("SqlServerConnectionString", resourceCulture);
            }
        }
    }
}
