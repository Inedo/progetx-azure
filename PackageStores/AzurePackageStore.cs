using System.ComponentModel;
using Inedo.Documentation;
using Inedo.ProGet.Extensibility.FileSystems;
using Inedo.ProGet.Extensibility.PackageStores;
using Inedo.Serialization;

namespace Inedo.ProGet.Extensions.Azure.PackageStores
{
    [DisplayName("Microsoft Azure")]
    [Description("A package store backed by Microsoft Azure Blob Store.")]
    [PersistFrom("Inedo.ProGet.Extensions.PackageStores.Azure.AzurePackageStore,ProGetCoreEx")]
    public sealed class AzurePackageStore : FileSystemPackageStore
    {
        private readonly AzureFileSystem fileSystem = new AzureFileSystem();
        public override FileSystem FileSystem => this.fileSystem;

        [Required]
        [Persistent]
        [DisplayName("Connection string")]
        [Description("A Microsoft Azure connection string, like <code>DefaultEndpointsProtocol=https;AccountName=account-name;AccountKey=account-key</code>")]
        public string ConnectionString
        {
            get => this.fileSystem.ConnectionString;
            set => this.fileSystem.ConnectionString = value;
        }

        [Required]
        [Persistent]
        [DisplayName("Container")]
        [Description("The name of the Azure Blob Container that will receive the uploaded files.")]
        public string ContainerName
        {
            get => this.fileSystem.ContainerName;
            set => this.fileSystem.ContainerName = value;
        }

        [Persistent]
        [DisplayName("Target path")]
        [Description("The path in the specified Azure Blob Container that will received the uploaded files; the default is the root.")]
        public string TargetPath
        {
            get => this.fileSystem.TargetPath;
            set => this.fileSystem.TargetPath = value;
        }
    }
}
