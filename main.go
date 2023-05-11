package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

var (
	accessKeyID     string
	accessKeySecret string
	endpoint        string
	bucketName      string
	objectName      string
	filePath        string
)

// 分片大小为1GB
const partSize = 1 << 30 // 1GB

// init initializes accessKeyID, accessKeySecret, endpoint, bucketName, objectName, and filePath
// by retrieving the values from the environment variables, command line arguments, and flags.
func init() {
	accessKeyID = os.Getenv("ACCESS_KEY")
	accessKeySecret = os.Getenv("ACCESS_SECRET")
	flag.StringVar(&endpoint, "endpoint", "", "OSS endpoint")
	flag.StringVar(&bucketName, "bucket", "", "Bucket name")
	flag.StringVar(&objectName, "object", "", "Object name")
	flag.StringVar(&filePath, "file", "", "File path")
	flag.Parse()
}

// main is the entry point of the program.
//
// It initializes the program, checks the parameters, creates an OSSClient
// instance, gets the bucket, uploads the file, and prints a success message.
// It exits with -1 if any error occurs.
func main() {
	// Check parameters
	if accessKeyID == "" || accessKeySecret == "" || bucketName == "" || objectName == "" || filePath == "" {
		fmt.Println("missing parameters")
		os.Exit(-1)
	}

	// Create an OSSClient instance.
	client, err := oss.New(endpoint, accessKeyID, accessKeySecret)
	if err != nil {
		fmt.Println("error: ", err)
		os.Exit(-1)
	}

	// Get the bucket.
	bucket, err := client.Bucket(bucketName)
	if err != nil {
		fmt.Println("error: ", err)
		os.Exit(-1)
	}

	// Upload the file.
	stat, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("error: file not exists [%s]", filePath)
			os.Exit(-1)
		}
	}

	// 判断文件大小，如果大于1G，则使用分片上传，否则使用简单上传。
	fileSize := stat.Size()
	if fileSize > partSize {
		err = multipartUpload(bucket, filePath, objectName)
	} else {
		err = simpleUpload(bucket, filePath, objectName)
	}
	if err != nil {
		fmt.Println("error: ", err)
		os.Exit(-1)
	}

	fmt.Println("upload success!")
}

// 定义进度条监听器。
type OssProgressListener struct {
}

// 定义进度变更事件处理函数。
func (listener *OssProgressListener) ProgressChanged(event *oss.ProgressEvent) {
	switch event.EventType {
	case oss.TransferStartedEvent:
		fmt.Printf("started, consumed bytes: %d, total bytes: %d.\n",
			event.ConsumedBytes, event.TotalBytes)
	case oss.TransferDataEvent:
		fmt.Printf("\ruploading consumed bytes: %d, total bytes: %d, %d%%.",
			event.ConsumedBytes, event.TotalBytes, event.ConsumedBytes*100/event.TotalBytes)
	case oss.TransferCompletedEvent:
		fmt.Printf("\ncompleted, consumed bytes: %d, total bytes: %d.\n",
			event.ConsumedBytes, event.TotalBytes)
	case oss.TransferFailedEvent:
		fmt.Printf("\nfailed, consumed bytes: %d, total bytes: %d.\n\n",
			event.ConsumedBytes, event.TotalBytes)
	default:
	}
}

// simpleUpload uploads a file to an OSS bucket using the specified bucket object and file paths.
//
// bucket: An *oss.Bucket object representing the OSS bucket to upload to.
// filePath: A string representing the local file path to upload from.
// objPath: A string representing the object path to upload to in the OSS bucket.
// Returns an error if the upload fails.
func simpleUpload(bucket *oss.Bucket, filePath, objPath string) error {
	return bucket.PutObjectFromFile(objPath, filePath, oss.Progress(&OssProgressListener{}))
}

// multipartUpload uploads a large file to an OSS bucket using multipart upload.
//
// bucket: an OSS bucket object that will receive the uploaded file parts.
// filePath: a string of the local file path to be uploaded.
// objPath: a string of the object path to be created in the bucket.
//
// Returns an error if the upload fails.
func multipartUpload(bucket *oss.Bucket, filePath, objPath string) error {
	stat, _ := os.Stat(filePath)
	splitParts := stat.Size() / partSize
	if stat.Size()%partSize != 0 {
		splitParts++
	}

	chunks, err := oss.SplitFileByPartNum(filePath, int(splitParts))
	if err != nil {
		return err
	}

	fd, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fd.Close()

	// 步骤1：初始化一个分片上传事件。
	options := []oss.Option{}
	imur, err := bucket.InitiateMultipartUpload(objectName, options...)
	if err != nil {
		return err
	}

	// 步骤2：上传分片。
	var parts []oss.UploadPart
	fmt.Println("start upload parts, total: ", len(chunks))
	for _, chunk := range chunks {
		fmt.Printf("upload part %d\n", chunk.Number)

		// fd.Seek(chunk.Offset, os.SEEK_SET)
		fd.Seek(chunk.Offset, io.SeekStart)
		// 调用UploadPart方法上传每个分片。
		part, err := bucket.UploadPart(imur, fd, chunk.Size, chunk.Number, oss.Progress(&OssProgressListener{}))
		if err != nil {
			return err
		}
		parts = append(parts, part)
	}

	// 步骤2：完成分片上传。
	_, err = bucket.CompleteMultipartUpload(imur, parts)
	if err != nil {
		return err
	}

	return nil
}
