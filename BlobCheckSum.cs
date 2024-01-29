using System.Security.Cryptography;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace BlobCheckSumFunction
{
    public class BlobCheckSum(ILogger<BlobCheckSum> logger)
    {
        private const string KeyValue = "HashString";
        private readonly ILogger<BlobCheckSum> _logger = logger;

        [Function(nameof(BlobCheckSum))]
        public async Task Run(
            [BlobTrigger("backups/{name}", Connection ="AzureWebJobsStorage")] BlobClient blob
        )
        {
            var checkSum = await GetCheckSum(blob);
            
            BlobProperties  properties = await blob.GetPropertiesAsync();

            if(DoesCheckSumMetaDataAlreadyExistAndMatch(properties, checkSum)){
                return;
            }

            await AddCheckSumToMetaData(properties.Metadata, checkSum, blob);
        }

        private static bool DoesCheckSumMetaDataAlreadyExistAndMatch(BlobProperties  properties, string checkSum) => properties.Metadata.TryGetValue(KeyValue, out var currentCheckSum) && currentCheckSum.Equals(checkSum);

        private static async Task AddCheckSumToMetaData(IDictionary<string, string>  metaData, string checkSum, BlobClient blob){
            metaData[KeyValue] = checkSum;

            await blob.SetMetadataAsync(metaData);
        }

        private static async Task<string> GetCheckSum(BlobClient blob)
        {
            using var stream = await blob.OpenReadAsync();
            using var sha = SHA512.Create();
            return Convert.ToBase64String(sha.ComputeHash(stream));
        }
    }
}