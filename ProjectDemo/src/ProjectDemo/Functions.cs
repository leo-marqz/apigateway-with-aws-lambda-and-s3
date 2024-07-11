using Amazon.Lambda.Annotations;
using Amazon.Lambda.Annotations.APIGateway;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.WebUtilities;
using System.Net.Http.Headers;
using System.Text.Json;


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
        [HttpApi(LambdaHttpMethod.Get, "/api/s3/list-all-buckets")]
        public async Task<APIGatewayProxyResponse> ListBuckets(ILambdaContext context)
        {
            try
            {
                ListBucketsResponse listBuckets = await this.s3.ListBucketsAsync();
                var buckets = listBuckets.Buckets.Select(bk=>bk.BucketName).ToList();

                return new APIGatewayProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonSerializer.Serialize(new
                    {
                        buckets = buckets
                    }),
                };
            }catch (Exception ex)
            {
                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonSerializer.Serialize(new
                    {
                        message = "Algo salio mal, revisar solicitud!"
                    })
                };
            }
        }

        [LambdaFunction()]
        [HttpApi(LambdaHttpMethod.Get, "/api/s3/{bucket}/list-all-objects")]
        public async Task<APIGatewayProxyResponse> ListObjectsFromBucket(string bucket, ILambdaContext context)
        {
            try
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrWhiteSpace(bucket))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonSerializer.Serialize(new
                        {
                            message = "El nombre del bucket es requerido!"
                        })
                    };
                }

                var request = new ListObjectsV2Request { BucketName = bucket };
                var response = await this.s3.ListObjectsV2Async(request);

                var objectsS3 = response.S3Objects.Select(obj => new
                {
                    key = obj.Key,
                    storage = obj.StorageClass.Value,
                    size = obj.Size,
                    bucket = obj.BucketName
                }).ToList();

                return new APIGatewayProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonSerializer.Serialize(new
                    {
                        objects = objectsS3
                    }),
                };
            }
            catch (Exception ex)
            {
                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonSerializer.Serialize(new
                    {
                        message = "Algo salio mal!"
                    })
                };
            }
        }

        [LambdaFunction()]
        [HttpApi(LambdaHttpMethod.Post, "/api/s3/{bucket}/upload-object")]
        public async Task<APIGatewayProxyResponse> UploadObjectToBucket(string bucket, APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
        {
            try
            {

                if (string.IsNullOrEmpty(bucket) || string.IsNullOrWhiteSpace(bucket))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonSerializer.Serialize(new
                        {
                            message = "El nombre del bucket es requerido!"
                        })
                    };
                }

                if (!request.Headers.ContainsKey("content-type"))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonSerializer.Serialize(new
                        {
                            message = "Encabezado content-type no valido!"
                        })
                    };
                }

                var boundary = request.Headers["content-type"].Split(";")[1].TrimStart().Split("=")[1];
                var contentBytes = Convert.FromBase64String(request.Body);
                var memory = new MemoryStream(contentBytes, 0, contentBytes.Count());
                var multipartReader = new MultipartReader(boundary, memory);
                multipartReader.BodyLengthLimit = this.multipartBodyLengthLimit;
                var section = await multipartReader.ReadNextSectionAsync();

                byte[] bt = Convert.FromBase64String(request.Body);

                if (section != null)
                {
                    var hasContentDispositionHeader = ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out var contentDisposition);
                    if (hasContentDispositionHeader && !string.IsNullOrEmpty(contentDisposition!.FileName))
                    {
                        string s3Key = "";
                        bool flkeepName = Boolean.Parse(request.Headers["keep-original-file-name"]);
                        string flname = request.Headers["form-file-name"];

                        if (flkeepName && !string.IsNullOrEmpty(flname))
                        {
                            s3Key = flname.ToLower().Replace(" ", "-");
                        }
                        else
                        {
                            var fileExtension = Path.GetExtension(contentDisposition.FileName);
                            s3Key = $"{Guid.NewGuid()}{fileExtension}";
                            s3Key = s3Key.Substring(0, s3Key.Length - 1);
                        }

                        var inputStream = new MemoryStream();
                        await section.Body.CopyToAsync(inputStream);

                        var uploadRequest = new PutObjectRequest
                        {
                            BucketName = bucket,
                            Key = s3Key,
                            InputStream = inputStream,
                            ContentType = section.ContentType
                        };

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
                                        key = s3Key,
                                        type = section.ContentType,
                                        message = "El archivo se cargo correctamente en S3!"
                                    })
                                };
                            }
                            else
                            {
                                return new APIGatewayProxyResponse
                                {
                                    StatusCode = (int)response.HttpStatusCode,
                                    Body =JsonSerializer.Serialize(new
                                    {
                                        message = "Error al intentar subir el archivo a S3!"
                                    })
                                };
                            }
                        }
                    }
                }

                return new APIGatewayProxyResponse
                {
                    StatusCode = 400,
                    Body = JsonSerializer.Serialize(new
                    {
                        message = "Error en la solicitud!"
                    })
                };

            }
            catch (Exception ex)
            {
                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonSerializer.Serialize(new
                    {
                        message = "Algo salio mal!"
                    })
                };
            }
        }

        [LambdaFunction()]
        [HttpApi(LambdaHttpMethod.Get, "/api/s3/{bucket}/download-object/{key}")]
        public async Task<APIGatewayProxyResponse> DownloadObjectFromBucket(string bucket, string key, ILambdaContext context)
        {
            try
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrWhiteSpace(bucket) || string.IsNullOrEmpty(key) || string.IsNullOrWhiteSpace(key))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonSerializer.Serialize(new
                        {
                            message = "El nombre del bucket es requerido!"
                        })
                    };
                }

                var request = new GetObjectRequest
                {
                    BucketName = bucket,
                    Key = key
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
                            { "Content-Disposition", $"attachment; filename={key}" },
                            { "Content-Transfer-Encoding", "base64" }
                        },
                        IsBase64Encoded = true
                    };
                };

            }
            catch (AmazonS3Exception ex)
            {
                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonSerializer.Serialize(new
                    {
                        message = "Algo salio mal!"
                    })
                };
            }
        }


    }

}