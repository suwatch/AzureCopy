using System;
using System.IO;
using System.Net;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Auth.Protocol;

namespace AzureCopy
{
    internal static class Program
    {
        static void Main(string[] args)
        {
            try
            {
                // DefaultEndpointsProtocol=https;AccountName=name;AccountKey=key
                var srcConnectionString = string.Empty;
                var dstConnectionString = string.Empty;

                // for blob token usage, source requires 'Storage Blob Data Reader' RBAC
                // for blob token usage, destination requires 'Storage Blob Data Contributor' RBAC
                // for file token usage, source requires 'Storage File Data Privileged Reader' RBAC
                // for file token usage, destination requires 'Storage File Data Privileged Contributor' RBAC
                var srcAccountName = string.Empty;
                var srcToken = string.Empty;
                var dstAccountName = string.Empty;
                var dstToken = string.Empty;

                var srcContainerName = string.Empty;
                var srcShareName = string.Empty;
                var srcLocalPath = string.Empty; // c:\temp\srcpath
                var srcRelativePath = string.Empty; // foo/bar

                var dstContainerName = string.Empty;
                var dstShareName = string.Empty;
                var dstLocalPath = string.Empty; // c:\temp\srcpath
                var dstRelativePath = string.Empty; // foo/bar

                // Create blob directory source using connectionString
                var src = GetTestCloudBlobDirectory(
                    connectionString: srcConnectionString,
                    containerName: srcContainerName,
                    relativePath: srcRelativePath,
                    createIfNotExists: false);

                // Create blob directory source using token
                //var src = GetTestCloudBlobDirectory(
                //    accountName: srcAccountName,
                //    token: srcToken,
                //    containerName: srcContainerName,
                //    relativePath: srcRelativePath,
                //    createIfNotExists: false);

                // Create local directory source
                //var src = GetTestLocalDirectory(
                //    path: srcLocalPath,
                //    createIfNotExists: false);


                // Create file directory destination using connectionString
                var dst = GetTestCloudFileDirectory(
                    connectionString: dstConnectionString,
                    shareName: dstShareName,
                    relativePath: dstRelativePath,
                    createIfNotExists: true);

                // Create file directory destination using token
                // 'Storage File Data Privileged Contributor' RBAC is required for write access
                // 'Storage Blob Data Contributor' RBAC is required for write access
                //var dst = GetTestCloudFileDirectory(
                //    accountName: dstAccountName,
                //    token: dstToken,
                //    shareName: dstShareName,
                //    relativePath: dstRelativePath,
                //    createIfNotExists: true);

                // Create local directory destination
                //var dst = GetTestLocalDirectory(
                //    path: dstLocalPath,
                //    createIfNotExists: true);

                // recursively copy source to destination directory
                src.Copyto(dst);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        static CloudBlobDirectory GetTestCloudBlobDirectory(string connectionString, string containerName, string relativePath, bool createIfNotExists = false)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (createIfNotExists)
            {
                container.CreateIfNotExists();
            }

            return container.GetDirectoryReference(relativePath);
        }

        static CloudBlobDirectory GetTestCloudBlobDirectory(string accountName, string token, string containerName, string relativePath, bool createIfNotExists = false)
        {
            var account = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={Convert.ToBase64String(Guid.NewGuid().ToByteArray())}");
            account = new CloudStorageAccount(new StorageCredentials(new TokenCredential(token)), account.BlobEndpoint, account.QueueEndpoint, account.TableEndpoint, account.FileEndpoint);
            var client = account.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            if (createIfNotExists)
            {
                container.CreateIfNotExists();
            }

            return container.GetDirectoryReference(relativePath);
        }

        static CloudFileDirectory GetTestCloudFileDirectory(string connectionString, string shareName, string relativePath, bool createIfNotExists = false)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            var client = account.CreateCloudFileClientEx();
            var share = client.GetShareReference(shareName);
            var root = share.GetRootDirectoryReference();
            if (createIfNotExists)
            {
                share.CreateIfNotExists();
                if (!string.IsNullOrEmpty(relativePath))
                {
                    var parts = relativePath.Split('/');
                    string currentPath = null;
                    for (int i = 0; i < parts.Length; i++)
                    {
                        currentPath = currentPath == null ? parts[i] : $"{currentPath}/{parts[i]}";
                        var dir = root.GetDirectoryReference(currentPath);
                        dir.CreateIfNotExists();
                    }
                }
            }

            return string.IsNullOrEmpty(relativePath) ? root : root.GetDirectoryReference(relativePath);
        }

        static CloudFileDirectory GetTestCloudFileDirectory(string accountName, string token, string shareName, string relativePath, bool createIfNotExists = false)
        {
            var account = CloudStorageAccount.Parse($"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={Convert.ToBase64String(Guid.NewGuid().ToByteArray())}");
            account = new CloudStorageAccount(new StorageCredentials(new TokenCredential(token)), account.BlobEndpoint, account.QueueEndpoint, account.TableEndpoint, account.FileEndpoint);
            var client = account.CreateCloudFileClientEx();
            var share = client.GetShareReference(shareName);
            var root = share.GetRootDirectoryReference();
            if (createIfNotExists)
            {
                // This API does not support bearer tokens. For OAuth, use the Storage Resource Provider APIs instead. Learn more: https://aka.ms/azurefiles/restapi.
                // share.CreateIfNotExists();

                if (!string.IsNullOrEmpty(relativePath))
                {
                    var parts = relativePath.Split('/');
                    string currentPath = null;
                    for (int i = 0; i < parts.Length; i++)
                    {
                        currentPath = currentPath == null ? parts[i] : $"{currentPath}/{parts[i]}";
                        var dir = root.GetDirectoryReference(currentPath);
                        dir.CreateIfNotExists();
                    }
                }
            }

            return string.IsNullOrEmpty(relativePath) ? root : root.GetDirectoryReference(relativePath);
        }

        static DirectoryInfo GetTestLocalDirectory(string path, bool createIfNotExists = false)
        {
            var dir = new DirectoryInfo(path);
            if (createIfNotExists)
            {
                dir.Create();
            }

            return dir;
        }

        static CloudFileClient CreateCloudFileClientEx(this CloudStorageAccount storageAccount)
        {
            var fileClient = storageAccount.CreateCloudFileClient();
            if (!storageAccount.Credentials.IsToken)
            {
                return fileClient;
            }

            var tokenAuthenticationHandlerType = fileClient.GetType().Assembly.GetType("Microsoft.WindowsAzure.Storage.Auth.Protocol.TokenAuthenticationHandler");
            var tokenAuthenticationHandlerConstructor = tokenAuthenticationHandlerType.GetConstructor(new[] { typeof(StorageCredentials) });
            var tokenAuthenticationHandler = tokenAuthenticationHandlerConstructor.Invoke(new[] { storageAccount.Credentials });
            var cloudTableClientAuthenticationHandlerField = fileClient.GetType().GetField("authenticationHandler", BindingFlags.Instance | BindingFlags.NonPublic);
            cloudTableClientAuthenticationHandlerField.SetValue(fileClient, new CloudFileAuthenticationHandler((IAuthenticationHandler)tokenAuthenticationHandler));

            return fileClient;
        }

        class CloudFileAuthenticationHandler : IAuthenticationHandler
        {
            readonly IAuthenticationHandler _inner;

            public CloudFileAuthenticationHandler(IAuthenticationHandler inner)
            {
                _inner = inner;
            }

            public void SignRequest(HttpWebRequest request, OperationContext operationContext)
            {
                _inner.SignRequest(request, operationContext);

                // override headers to support AzureFiles REST with EntraID token
                request.Headers["x-ms-version"] = "2022-11-02";
                request.Headers["x-ms-file-request-intent"] = "backup";
            }
        }
    }
}
