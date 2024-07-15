using Amazon;
using Amazon.Lambda.Annotations;
using Amazon.Lambda.Annotations.APIGateway;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Amazon.S3;
using Amazon.S3.Model;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.WebUtilities;
using System.Collections;
using System.Net.Http.Headers;
using System.Text;
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
            this.bucketName = "tb-bucket-s3";
            this.s3 = new AmazonS3Client();
            this.multipartBodyLengthLimit = 100 * 1024 * 1024; // 100 MB

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
                    Body = JsonSerializer.Serialize(buckets),
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


                var request = new ListObjectsV2Request { 
                    BucketName = bucket
                };
                var response = await this.s3.ListObjectsV2Async(request);

                var objectsS3 = response.S3Objects.Select(obj => new
                {
                    key = obj.Key,
                    storage = obj.StorageClass.Value,
                    size = obj.Size,
                    bucket = obj.BucketName
                }).OrderByDescending((obj)=>obj.key).ToList();

                return new APIGatewayProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonSerializer.Serialize(objectsS3),
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
        [HttpApi(LambdaHttpMethod.Get, "/api/s3/{bucket}/gen-presigned-url/object")]
        public async Task<APIGatewayProxyResponse> GetPreSignedUrlTypeGET(string bucket, APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
        {
            try
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrWhiteSpace(bucket) )
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

                if (!request.Headers.ContainsKey("s3-object-key"))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonSerializer.Serialize(new
                        {
                            message = "La key o nombre de archivo es requerido!"
                        })
                    };
                }

                var requestS3 = new GetPreSignedUrlRequest()
                {
                    BucketName = bucket,
                    Key = request.Headers["s3-object-key"],
                    Verb = HttpVerb.GET,
                    Expires = DateTime.Now.AddHours(5)
                };


                string urlString = this.s3.GetPreSignedURL(requestS3);


                if (string.IsNullOrEmpty(urlString))
                {
                    new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = "Error en la solicitud!"
                    };
                }

                return new APIGatewayProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonSerializer.Serialize(new
                    {
                        key = request.Headers["s3-object-key"],
                        url = urlString,
                        expire = requestS3.Expires.ToLocalTime(),
                    })
                };
            }
            catch (Exception ex)
            {
                return new APIGatewayProxyResponse
                {
                    StatusCode = 500,
                    Body = "Ups, Algo salio mal"
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
                        Body = "El nombre del bucket es requerido!"
                    };
                }

                if (!request.Headers.ContainsKey("content-type"))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = "Encabezado content-type no valido!"
                    };
                }

                var boundary = request.Headers["content-type"].Split(";")[1].TrimStart().Split("=")[1];
                var contentBytes = Convert.FromBase64String(request.Body);
                var memory = new MemoryStream(contentBytes, 0, contentBytes.Count());
                var multipartReader = new MultipartReader(boundary, memory);
                multipartReader.BodyLengthLimit = this.multipartBodyLengthLimit;

                var section = await multipartReader.ReadNextSectionAsync();

                var objects = new List<dynamic>();

                while (section != null)
                {
                    var hasContentDispositionHeader = ContentDispositionHeaderValue.TryParse(section.ContentDisposition, out var contentDisposition);
                    if (hasContentDispositionHeader && !string.IsNullOrEmpty(contentDisposition!.FileName))
                    {
                        var filename = Path.GetFileName(contentDisposition.FileName);
                        var key = filename.ToLower().Replace(" ", "-").Replace("\"", "");
                        
                        using(var inputStream = new  MemoryStream())
                        {
                            await section.Body.CopyToAsync(inputStream);
                            inputStream.Position = 0;

                            var uploadRequest = new PutObjectRequest
                            {
                                BucketName = bucket,
                                Key = key,
                                InputStream = inputStream,
                                ContentType = section.ContentType
                            };

                            PutObjectResponse response = await this.s3.PutObjectAsync(uploadRequest);

                            if(response != null)
                            {
                                if(response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                                {
                                    objects.Add(new
                                    {
                                        filename = filename,
                                        key = key,
                                        statusCode = (int)response.HttpStatusCode
                                    });
                                }
                                else
                                {
                                    objects.Add(new
                                    {
                                        filename = filename,
                                        key = key,
                                        statusCode = (int)response.HttpStatusCode
                                    });
                                }
                            }
                            else
                            {
                                objects.Add(new
                                {
                                    filename = filename,
                                    key = key,
                                    statusCode = 500
                                });
                            }

                        } //end memory stream

                    } //end if with hasContentDispositionHeader 

                    section = await multipartReader.ReadNextSectionAsync();
                } //end while

                return new APIGatewayProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonSerializer.Serialize(objects)
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
        [HttpApi(LambdaHttpMethod.Get, "/api/s3/{bucket}/download-object")]
        public async Task<APIGatewayProxyResponse> DownloadObjectFromBucket(string bucket, APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
        {
            try
            {
                if (string.IsNullOrEmpty(bucket) || string.IsNullOrWhiteSpace(bucket) )
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

                if (!request.Headers.ContainsKey("s3-object-key"))
                {
                    return new APIGatewayProxyResponse
                    {
                        StatusCode = 400,
                        Body = JsonSerializer.Serialize(new
                        {
                            message = "La key o nombre de archivo es requerido!"
                        })
                    };
                }

                var requestS3 = new GetObjectRequest
                {
                    BucketName = bucket,
                    Key = request.Headers["s3-object-key"]
                };

                var filename = request.Headers["s3-object-key"].Split("/").Last();

                using (var response = await this.s3.GetObjectAsync(requestS3))
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
                            { "Content-Disposition", $"attachment; filename={filename}" },
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
                    Body = "Algo salio mal!"
                };
            }
        }


    }

}