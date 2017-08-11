using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Inedo.Documentation;
using Inedo.IO;
using Inedo.ProGet.Extensibility.FileSystems;
using Inedo.Serialization;

namespace Inedo.ProGet.Extensions.Azure.PackageStores
{
    public sealed class AzureFileSystem : FileSystem
    {
        private static readonly LazyRegex MultiSlashPattern = new LazyRegex(@"/{2,}");

        private Lazy<CloudStorageAccount> cloudStorageAccount;
        private Lazy<CloudBlobClient> cloudBlobClient;
        private Lazy<CloudBlobContainer> cloudBlobContainer;

        public AzureFileSystem()
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

        private string BuildPath(string path)
        {
            // Collapse slashes.
            path = MultiSlashPattern.Replace(path.Trim('/'), "");

            return this.Prefix + path;
        }

        public async override Task<Stream> OpenFileAsync(string fileName, FileMode mode, FileAccess access, FileShare share, bool requireRandomAccess)
        {
            var path = this.BuildPath(fileName);
            var blob = await this.Container.GetBlobReferenceFromServerAsync(path).ConfigureAwait(false);

            if (mode == FileMode.Open && access == FileAccess.Read && !requireRandomAccess)
            {
                if (!await blob.ExistsAsync().ConfigureAwait(false))
                {
                    throw new FileNotFoundException("File not found: " + fileName, fileName);
                }

                // Fast path: just download as a stream
                return await blob.OpenReadAsync().ConfigureAwait(false);
            }

            bool? wantExisting;
            bool loadExisting;
            bool seekToEnd;
            switch (mode)
            {
                case FileMode.CreateNew:
                    wantExisting = true;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.Create:
                    wantExisting = null;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.Open:
                    wantExisting = false;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.OpenOrCreate:
                    wantExisting = false;
                    loadExisting = true;
                    seekToEnd = false;
                    break;
                case FileMode.Truncate:
                    wantExisting = true;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.Append:
                    wantExisting = null;
                    loadExisting = true;
                    seekToEnd = true;
                    break;
                default:
                    throw new NotSupportedException("Unsupported FileMode: " + mode.ToString());
            }

            Stream stream = null;

            if (loadExisting)
            {
                if (!await blob.ExistsAsync().ConfigureAwait(false))
                {
                    if (wantExisting == true)
                    {
                        throw new FileNotFoundException("File not found: " + fileName, fileName);
                    }
                }
                else
                {
                    using (var responseStream = await blob.OpenReadAsync().ConfigureAwait(false))
                    {
                        stream = TemporaryStream.Create(blob.Properties.Length);
                        try
                        {
                            await responseStream.CopyToAsync(stream).ConfigureAwait(false);
                        }
                        catch
                        {
                            try { stream.Dispose(); } catch { }
                            stream = null;
                            throw;
                        }
                    }
                }
            }
            else if (wantExisting.HasValue)
            {
                var exists = await blob.ExistsAsync().ConfigureAwait(false);
                if (exists && wantExisting == false)
                {
                    throw new IOException("File already exists: " + fileName);
                }
                else if (!exists && wantExisting == true)
                {
                    throw new FileNotFoundException("File not found: " + fileName, fileName);
                }
            }

            if (stream == null)
            {
                stream = TemporaryStream.Create(10 * 1024 * 1024);
            }
            else
            {
                try
                {
                    stream.Seek(0, seekToEnd ? SeekOrigin.End : SeekOrigin.Begin);
                }
                catch
                {
                    try { stream.Dispose(); } catch { }
                    throw;
                }
            }

            return new AzureStream(this, stream, path, (access & FileAccess.Write) != 0);
        }

        public override async Task CopyFileAsync(string sourceName, string targetName, bool overwrite)
        {
            var source = this.Container.GetBlobReference(this.BuildPath(sourceName));
            var target = this.Container.GetBlobReference(this.BuildPath(targetName));
            if (!overwrite && await target.ExistsAsync().ConfigureAwait(false))
            {
                throw new IOException("Destination file exists, but overwrite is not allowed: " + targetName);
            }
            await target.StartCopyAsync(source.Uri).ConfigureAwait(false);
            while (target.CopyState?.Status == CopyStatus.Pending)
            {
                await Task.Delay(1000).ConfigureAwait(false);
                await target.FetchAttributesAsync().ConfigureAwait(false);
            }
        }

        public override async Task DeleteFileAsync(string fileName)
        {
            await this.Container.GetBlobReference(this.BuildPath(fileName)).DeleteIfExistsAsync().ConfigureAwait(false);
        }

        public override Task CreateDirectoryAsync(string directoryName)
        {
            return InedoLib.NullTask;
        }

        public async override Task DeleteDirectoryAsync(string directoryName, bool recursive)
        {
            if (!recursive)
            {
                return;
            }

            var directory = this.Container.GetDirectoryReference(this.BuildPath(directoryName));
            var files = directory.ListBlobs(true);
            foreach (var file in files)
            {
                var blob = file as CloudBlob;
                if (blob != null)
                {
                    await blob.DeleteAsync().ConfigureAwait(false);
                }
            }
        }

        public override Task<IEnumerable<FileSystemItem>> ListContentsAsync(string path)
        {
            var directory = this.Container.GetDirectoryReference(this.BuildPath(path));
            var files = directory.ListBlobs();
            var contents = new List<FileSystemItem>();
            foreach (var file in files)
            {
                var dir = file as CloudBlobDirectory;
                if (dir != null)
                {
                    contents.Add(new AzureFileSystemItem(PathEx.GetFileName(dir.Prefix)));
                }
                var blob = file as ICloudBlob;
                if (blob != null)
                {
                    contents.Add(new AzureFileSystemItem(PathEx.GetFileName(blob.Name), blob.Properties.Length));
                }
            }

            return Task.FromResult((IEnumerable<FileSystemItem>)contents);
        }

        public async override Task<FileSystemItem> GetInfoAsync(string path)
        {
            var file = await this.Container.GetBlobReferenceFromServerAsync(this.BuildPath(path)).ConfigureAwait(false);
            if (!await file.ExistsAsync().ConfigureAwait(false))
            {
                var directory = this.Container.GetDirectoryReference(this.BuildPath(path));
                var contents = await directory.ListBlobsSegmentedAsync(true, BlobListingDetails.None, 1, null, null, null).ConfigureAwait(false);
                if (contents.Results.Any())
                {
                    return new AzureFileSystemItem(PathEx.GetFileName(path));
                }
                return null;
            }

            return new AzureFileSystemItem(PathEx.GetFileName(path), file.Properties.Length);
        }

        private class AzureFileSystemItem : FileSystemItem
        {
            public AzureFileSystemItem(string name)
            {
                this.Name = name;
                this.Size = null;
                this.IsDirectory = true;
            }

            public AzureFileSystemItem(string name, long size)
            {
                this.Name = name;
                this.Size = size;
                this.IsDirectory = false;
            }

            public override string Name { get; }
            public override long? Size { get; }
            public override bool IsDirectory { get; }
        }

        private sealed class AzureStream : Stream
        {
            private readonly AzureFileSystem outer;
            private readonly Stream inner;
            private readonly string path;

            public AzureStream(AzureFileSystem outer, Stream inner, string path, bool canWrite)
            {
                this.outer = outer;
                this.inner = inner;
                this.path = path;
                this.CanWrite = canWrite;
            }

            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite { get; }
            public override long Length => this.inner.Length;
            public override long Position
            {
                get => this.inner.Position;
                set => this.inner.Position = value;
            }

            public override void Flush()
            {
                // no-op
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return this.inner.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return this.inner.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                this.inner.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                this.inner.Write(buffer, offset, count);
            }

            public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            {
                return this.inner.CopyToAsync(destination, bufferSize, cancellationToken);
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return this.inner.ReadAsync(buffer, offset, count, cancellationToken);
            }

            public override int ReadByte()
            {
                return this.inner.ReadByte();
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return this.inner.WriteAsync(buffer, offset, count, cancellationToken);
            }

            public override void WriteByte(byte value)
            {
                this.inner.WriteByte(value);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    using (this.inner)
                    {
                        if (this.CanWrite)
                        {
                            this.FinishUploadAsync().WaitAndUnwrapExceptions();
                        }
                    }
                }
                base.Dispose(disposing);
            }

            private async Task FinishUploadAsync()
            {
                await this.outer.Container.CreateIfNotExistsAsync().ConfigureAwait(false);
                using (var stream = await this.outer.Container.GetBlockBlobReference(this.path).OpenWriteAsync().ConfigureAwait(false))
                {
                    this.inner.Position = 0;
                    await this.inner.CopyToAsync(stream).ConfigureAwait(false);
                    stream.Commit();
                }
            }
        }
    }
}
