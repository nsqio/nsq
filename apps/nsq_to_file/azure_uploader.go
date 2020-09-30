package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/nsqio/nsq/internal/lg"
)

// AzureUploader struct definition
type AzureUploader struct {
	accountName   string
	accountKey    string
	containerName string
	events        chan string
	logf          lg.AppLogFunc

	containerURL azblob.ContainerURL
}

// NewAzureUploader function returns a new instance of AzureUploader
func NewAzureUploader(accountName, accountKey, containerName string) (*AzureUploader, error) {
	au := new(AzureUploader)
	au.accountName = accountName
	au.accountKey = accountKey
	au.containerName = containerName
	au.events = make(chan string)

	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", au.accountName, au.containerName))

	au.containerURL = azblob.NewContainerURL(*URL, p)

	return au, nil
}

func (au *AzureUploader) listenToEventsChannel() {

	for {
		select {
		case filePath := <-au.events:

			file, err := os.Open(filePath)

			stat, err := file.Stat()
			if err != nil {
				au.logf(lg.ERROR, "failed to open file in AzureUploader, listenToEventsChannel: %s", err)
				continue
			}

			// Check file size, if empty skip processing
			if stat.Size() == 0 {
				au.logf(lg.WARN, "File is empty, skipping")
				continue
			}

			if err != nil {
				au.logf(lg.ERROR, "failed to open file in AzureUploader, listenToEventsChannel: %s", err)
				continue
			}

			// eg files:
			// 8-bithumb-BTC-KRW.cc-md-nsq-output-05-000000.2020-09-29_13.log.gz
			// 30-bithumb-OMG-KRW.cc-md-nsq-output-05-000000.2020-09-29_13.log.gz
			// 0-bithumb.cc-md-nsq-output-05-000000.2020-09-29_13.log.gz

			fileName := path.Base(filePath)
			dashSplitted := strings.Split(fileName, "-")
			exchangeName := strings.ToLower(dashSplitted[1])

			// Begin blob path string construction
			blobPath := exchangeName

			if strings.HasPrefix(fileName, "8-") || strings.HasPrefix(fileName, "30-") {
				// Level2

				base := strings.ToLower(dashSplitted[2])
				quote := strings.ToLower(dashSplitted[3])

				blobPath = exchangeName
				if dashSplitted[0] == "8" {
					blobPath += "/obupdate/"
				} else {
					blobPath += "/topofbook/"
				}

				blobPath += base + "-" + quote + "/"

			} else if strings.HasPrefix(fileName, "0-") {
				// Trades
				blobPath += "/trade/"

			} else {
				// Unsupported
				au.logf(lg.ERROR, "Unsupported file type, filename [%s]", fileName)
				continue
			}

			// Parse date
			dateString := strings.SplitAfterN(fileName, ".", 2)[0]
			dateLayout := "2006-01-02_15"

			dateTime, err := time.Parse(dateLayout, dateString)
			if err != nil {
				au.logf(lg.ERROR, "parsing date time, %s", err)
				continue
			}

			blobPath += fmt.Sprintf("%d.gz", dateTime.Unix())

			// Create Block Blob Path
			blobURL := au.containerURL.NewBlockBlobURL(blobPath)

			_, err = azblob.UploadFileToBlockBlob(context.Background(), file, blobURL, azblob.UploadToBlockBlobOptions{
				BlockSize:   512 * 1024,
				Parallelism: 2})
			if err != nil {
				au.logf(lg.ERROR, "failed to upload in AzureUploader, listenToEventsChannel: %s", err)
			}

		}
	}

}
