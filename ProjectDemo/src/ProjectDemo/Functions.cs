using Amazon.Lambda.Annotations;
using Amazon.Lambda.Annotations.APIGateway;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.WebUtilities;
using System.Net.Http.Headers;
using System.Text.Json;
using static System.Collections.Specialized.BitVector32;
//using Microsoft.Net.Http.Headers;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ProjectDemo
{
    public class Functions
    {
        private readonly string bucketName;
        private readonly IAmazonS3 s3;
        private readonly long multipartBodyLengthLimit;


        public Functions()
        {
            this.bucketName = "leomarqz-storage";
            this.s3 = new AmazonS3Client();
            this.multipartBodyLengthLimit = 50 * 1024 * 1024; // 50 MB

            //"Role": "arn:aws:iam::471112617336:role/AWSLambdaWithAmazonS3Role",
        }

        [LambdaFunction()]
        [HttpApi(LambdaHttpMethod.Get, "/")]
        public async Task<APIGatewayProxyResponse> GetAll(ILambdaContext context)
        {
            try
            {
                var request = new ListObjectsV2Request { BucketName = this.bucketName };
                var response = await this.s3.ListObjectsV2Async(request);
                var objectNames = response.S3Objects.Select(obj => obj.Key).ToList();
                var result = new
                {
                    Objects = objectNames,
                    Bucket = this.bucketName
                };

                context.Logger.Log(JsonSerializer.Serialize(result));

                return new APIGatewayProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonSerializer.Serialize(result),
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }
            catch (Exception ex)
            {
                context.Logger.LogError($"Error listing objects from bucket {this.bucketName}: {ex.Message}");

                var errorResponse = new
                {
                    Message = $"Error listing objects from bucket {this.bucketName}",
                    Details = ex.Message
                };

                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonSerializer.Serialize(errorResponse),
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }
        }

        [LambdaFunction()]
        [HttpApi(LambdaHttpMethod.Get, "/download/key/{keyname}")]
        public async Task<APIGatewayProxyResponse> Download(string keyname, ILambdaContext context)
        {
            try
            {
                var request = new GetObjectRequest
                {
                    BucketName = this.bucketName,
                    Key = keyname
                };

                using (var response = await this.s3.GetObjectAsync(request))
                using (var responseStream = response.ResponseStream)
                using (var memoryStream = new MemoryStream())
                {
                    await responseStream.CopyToAsync(memoryStream);
                    var contentType = response.Headers["Content-Type"];
                    var base64 = Convert.ToBase64String(memoryStream.ToArray());

                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 200,
                        Body = base64,
                        Headers = new Dictionary<string, string>
                        {
                            { "Content-Type", contentType },
                            { "Content-Disposition", $"attachment; filename={keyname}" },
                            { "Content-Transfer-Encoding", "base64" }
                        },
                        IsBase64Encoded = true
                    };
                };

            }
            catch (AmazonS3Exception ex)
            {
                context.Logger.LogError($"Error getting object {keyname} from bucket {this.bucketName}: {ex.Message}");

                var errorResponse = new
                {
                    Message = $"Error getting object {keyname} from bucket {this.bucketName}",
                    Details = ex.Message
                };

                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonSerializer.Serialize(errorResponse),
                    Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
                };
            }
        }

        [LambdaFunction()]
        [HttpApi(LambdaHttpMethod.Post, "/upload")]
        public async Task<APIGatewayProxyResponse> Upload(APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
        {
            try
            {
                if (!request.Headers.ContainsKey("content-type"))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = "Missing content-type header"
                    };
                }

                var boundary = request.Headers["content-type"].Split(";")[1].TrimStart().Split("=")[1];
                var contentBytes = Convert.FromBase64String(request.Body);
                var memory = new MemoryStream(contentBytes, 0, contentBytes.Count());
                var multipartReader = new MultipartReader(boundary, memory );
                multipartReader.BodyLengthLimit = this.multipartBodyLengthLimit;
                var section = await multipartReader.ReadNextSectionAsync();

                byte[] bt = Convert.FromBase64String(request.Body);

                if (section != null)
                {
                    var hasContentDispositionHeader = ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out var contentDisposition);
                    if(hasContentDispositionHeader && !string.IsNullOrEmpty(contentDisposition!.FileName))
                    {

                        var fileExtension = Path.GetExtension(contentDisposition.FileName);
                        var s3Key = $"{Guid.NewGuid()}{fileExtension}";
                        s3Key = s3Key.Substring(0, s3Key.Length - 1);

                        var inputStream = new MemoryStream();
                        await section.Body.CopyToAsync(inputStream);

                        var uploadRequest = new PutObjectRequest
                        {
                            BucketName = this.bucketName,
                            Key = s3Key,
                            InputStream = inputStream,
                            ContentType = section.ContentType
                        };

                        //string hasContent = "no hay contenido";

                        //if(memory.Length > 0)
                        //{
                        //    hasContent = "si hay contenido";
                        //}

                        PutObjectResponse response = await this.s3.PutObjectAsync(uploadRequest);

                        if (response != null)
                        {
                            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                            {
                                return new APIGatewayProxyResponse
                                {
                                    StatusCode = 200,
                                    Body = JsonSerializer.Serialize(new
                                    {
                                        Key = s3Key,
                                        Type = section.ContentType
                                    })
                                };
                            }
                            else
                            {
                                return new APIGatewayProxyResponse
                                {
                                    StatusCode = (int)response.HttpStatusCode,
                                    Body = "Error al subir el objeto a S3"
                                };
                            }
                        }
                    }
                }

                return new APIGatewayProxyResponse
                {
                    StatusCode = 400,
                    Body = "Error en la request, no hay contenido"
                };

            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Error: {ex.Message}");
                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = $"Error: {ex.Message} ||| Source: {ex.Source}"
                };
            }
        }

    }

}