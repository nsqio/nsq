package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

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
		case filename := <-au.events:
			// parse file name for pair and details
			blobURL := au.containerURL.NewBlockBlobURL(filename)
			file, err := os.Open(filename)
			if err != nil {
				au.logf(lg.ERROR, "failed to open file in AzureUploader, listenToEventsChannel: %s", err)
				continue
			}

			_, err = azblob.UploadFileToBlockBlob(context.Background(), file, blobURL, azblob.UploadToBlockBlobOptions{
				BlockSize:   512 * 1024,
				Parallelism: 2})
			if err != nil {
				au.logf(lg.ERROR, "failed to upload in AzureUploader, listenToEventsChannel: %s", err)
			}

		}
	}

}
