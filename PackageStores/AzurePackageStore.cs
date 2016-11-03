using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Inedo.Documentation;
using Inedo.ProGet.Extensibility.PackageStores;
using Inedo.Serialization;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Inedo.ProGet.Extensions.Azure.PackageStores
{
    [DisplayName("Microsoft Azure")]
    [Description("A package store backed by Microsoft Azure Blob Store.")]
    [PersistFrom("Inedo.ProGet.Extensions.PackageStores.Azure.AzurePackageStore,ProGetCoreEx")]
    public sealed partial class AzurePackageStore : CommonIndexedPackageStore
    {
        private Lazy<CloudStorageAccount> cloudStorageAccount;
        private Lazy<CloudBlobClient> cloudBlobClient;
        private Lazy<CloudBlobContainer> cloudBlobContainer;

        public AzurePackageStore()
        {
            this.cloudStorageAccount = new Lazy<CloudStorageAccount>(() => CloudStorageAccount.Parse(this.ConnectionString));
            this.cloudBlobClient = new Lazy<CloudBlobClient>(() => this.Account.CreateCloudBlobClient());
            this.cloudBlobContainer = new Lazy<CloudBlobContainer>(() => this.Client.GetContainerReference(this.ContainerName));
        }

        [Required]
        [Persistent]
        [DisplayName("Connection string")]
        [Description("A Microsoft Azure connection string, like <code>DefaultEndpointsProtocol=https;AccountName=account-name;AccountKey=account-key</code>")]
        public string ConnectionString { get; set; }

        [Required]
        [Persistent]
        [DisplayName("Container")]
        [Description("The name of the Azure Blob Container that will receive the uploaded files.")]
        public string ContainerName { get; set; }

        [Persistent]
        [DisplayName("Target path")]
        [Description("The path in the specified Azure Blob Container that will received the uploaded files; the default is the root.")]
        public string TargetPath { get; set; }

        private CloudStorageAccount Account => this.cloudStorageAccount.Value;
        private CloudBlobClient Client => this.cloudBlobClient.Value;
        private CloudBlobContainer Container => this.cloudBlobContainer.Value;
        private string Prefix => string.IsNullOrEmpty(this.TargetPath) || this.TargetPath.EndsWith("/") ? this.TargetPath : (this.TargetPath + "/");

        protected override Task<IEnumerable<string>> EnumerateFilesAsync(string extension)
        {
            return Task.FromResult(
                from b in this.Container.ListBlobs(prefix: this.Prefix, useFlatBlobListing: true)
                let name = (b as CloudBlockBlob)?.Name
                where !string.IsNullOrEmpty(name) && name.EndsWith(extension, StringComparison.OrdinalIgnoreCase)
                select name
            );
        }
        protected override async Task<Stream> OpenReadAsync(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var blob = this.Container.GetBlockBlobReference(path);
            if (!await blob.ExistsAsync().ConfigureAwait(false))
                return null;

            return await blob.OpenReadAsync().ConfigureAwait(false);
        }
        protected override async Task<Stream> CreateAsync(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            await this.Container.CreateIfNotExistsAsync().ConfigureAwait(false);
            var blob = this.Container.GetBlockBlobReference(path);
            return await blob.OpenWriteAsync().ConfigureAwait(false);
        }
        protected override Task DeleteAsync(string path)
        {
            if (string.IsNullOrEmpty(path))
                throw new ArgumentNullException(nameof(path));

            var blob = this.Container.GetBlockBlobReference(path);
            return blob.DeleteIfExistsAsync();
        }
        protected override async Task RenameAsync(string originalName, string newName)
        {
            if (string.IsNullOrEmpty(originalName))
                throw new ArgumentNullException(nameof(originalName));
            if (string.IsNullOrEmpty(newName))
                throw new ArgumentNullException(nameof(newName));

            if (string.Equals(originalName, newName, StringComparison.OrdinalIgnoreCase))
                return;

            var sourceBlob = this.Container.GetBlockBlobReference(originalName);
            var targetBlob = this.Container.GetBlockBlobReference(newName);
            await targetBlob.StartCopyAsync(sourceBlob).ConfigureAwait(false);
            await sourceBlob.DeleteAsync().ConfigureAwait(false);
        }
        protected override string GetFullPackagePath(PackageStorePackageId packageId) => this.Prefix + this.GetRelativePackagePath(packageId, '/');
    }
}
