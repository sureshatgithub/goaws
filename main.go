package main

import (
	"encoding/json"
	"io/ioutil"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Println("Please provide bucket and file name")
	}

	srcBucket := os.Args[1]
	srcName := os.Args[2]
	destBucket := os.Args[3]
	destName := os.Args[4]

	err := downloadFile(srcBucket, srcName)
	if err != nil {
		fmt.Println("Unable to download source file", srcName)
	}

	err = downloadFile(destBucket, destName)
	if err != nil {
		fmt.Println("Unable to download source file", destName)
	}

	success, err := compareJSON(srcName, destName)
	if err != nil {
		fmt.Println(err)
	} else {
		if success {
			fmt.Println("Json Values are equal")
		} else {
			fmt.Println("Json Values are not equal")
		}
	}
}

func compareJSON(srcName string, destName string) (bool, error) {
	srcJSON, err1 := getJSONFileContent(srcName)
	destJSON, err2 := getJSONFileContent(destName)
	if err1 == nil {
		fmt.Println(srcJSON)
	} else {
		fmt.Println("Unable to parse the source json data file", err1)
		return false, err1
	}

	if err2 == nil {
		fmt.Println(destJSON)
	} else {
		fmt.Println("Unable to parse dest json data file", err2)
		return false, err2
	}

	status, err3 := compareJSONValues(srcJSON, destJSON)

	return status, err3
}
func compareJSONValues(srcJSON string, destJSON string) (bool, error) {
	var o1 interface{}
	var o2 interface{}
	var err error
	err = json.Unmarshal([]byte(srcJSON), &o1)
	if err != nil {
		return false, fmt.Errorf("Error mashalling string 1 :: %s", err.Error())
	}
	err = json.Unmarshal([]byte(destJSON), &o2)
	if err != nil {
		return false, fmt.Errorf("Error mashalling string 2 :: %s", err.Error())
	}

	return reflect.DeepEqual(o1, o2), nil
}
func getJSONFileContent(filename string) (string, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Print(err)
		return "", err
	}
	return string(b), nil
}
func downloadFile(srcBucket string, fileName string) error {
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Unable to open file ", fileName, err)
	}

	defer file.Close()

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String("ap-south-1")},
	)

	downloader := s3manager.NewDownloader(sess)

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(srcBucket),
			Key:    aws.String(fileName),
		})
	if err != nil {
		fmt.Println("Unable to download item ", fileName, err)
		return err
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
	return nil
}
