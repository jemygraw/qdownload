package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

/*
the url list file format is:
url\tfilekey\tfsize
*/

func main() {
	var worker int
	var urlListFile string
	var destDir string
	var proxyHost string
	var referer string

	flag.IntVar(&worker, "worker", 1, "worker count")
	flag.StringVar(&urlListFile, "file", "", "url list file")
	flag.StringVar(&destDir, "dest", "", "dest dir")
	flag.StringVar(&proxyHost, "proxy", "", "http proxy host")
	flag.StringVar(&referer, "referer", "", "domain referer")
	flag.Parse()

	if urlListFile == "" {
		fmt.Println("Error: no url list file specified")
		return
	}

	if destDir == "" {
		fmt.Println("Error: no dest dir specified")
		return
	}

	if worker <= 0 {
		worker = 1
	}

	BatchDownload(worker, urlListFile, destDir, proxyHost, referer)
}

var downloadTasks chan func()
var initDownOnce sync.Once

func doDownload(tasks chan func()) {
	for {
		task := <-tasks
		task()
	}
}

func BatchDownload(worker int, urlListFile, destDir, proxyHost, referer string) {
	timeStart := time.Now()

	listFp, openErr := os.Open(urlListFile)
	if openErr != nil {
		log.Println("Error: open list file error", openErr)
		return
	}
	defer listFp.Close()
	listScanner := bufio.NewScanner(listFp)
	listScanner.Split(bufio.ScanLines)
	downWaitGroup := sync.WaitGroup{}

	totalCount := 0
	existsCount := 0

	var successCount int32
	var failCount int32

	initDownOnce.Do(func() {
		downloadTasks = make(chan func(), worker)
		for i := 0; i < worker; i++ {
			go doDownload(downloadTasks)
		}
	})

	for listScanner.Scan() {
		totalCount++
		line := strings.TrimSpace(listScanner.Text())
		items := strings.Split(line, "\t")
		if len(items) == 3 {
			fileURL := items[0]
			fileKey := items[1]
			fileSize, pErr := strconv.ParseInt(items[2], 10, 64)
			if pErr != nil {
				atomic.AddInt32(&failCount, 1)
				log.Println("Error: invalid line", line)
			}
			if !checkLocalDuplicate(destDir, fileKey, fileSize) {
				downWaitGroup.Add(1)
				downloadTasks <- func() {
					defer downWaitGroup.Done()
					downErr := downloadFile(destDir, fileURL, fileKey, proxyHost, referer)
					if downErr != nil {
						atomic.AddInt32(&failCount, 1)
					} else {
						atomic.AddInt32(&successCount, 1)
					}
				}
			} else {
				existsCount++
			}

		} else {
			atomic.AddInt32(&failCount, 1)
			log.Println("Error: invalid line", line)
		}
	}
	downWaitGroup.Wait()

	log.Println()
	log.Println("-------Download Result-------")
	log.Println("Total:\t", totalCount)
	log.Println("Local:\t", existsCount)
	log.Println("Success:\t", successCount)
	log.Println("Failure:\t", failCount)
	log.Println("Duration:\t", time.Since(timeStart))
	log.Println("-----------------------------")
}

func checkLocalDuplicate(destDir string, fileKey string, fileSize int64) bool {
	dup := false
	filePath := filepath.Join(destDir, fileKey)
	fStat, statErr := os.Stat(filePath)
	if statErr == nil {
		//exist, check file size
		localFileSize := fStat.Size()
		if localFileSize == fileSize {
			dup = true
		}
	}
	return dup
}

func downloadFile(destDir, fileURL, fileKey, proxyHost, referer string) (err error) {
	localFilePath := filepath.Join(destDir, fileKey)
	ldx := strings.LastIndex(localFilePath, string(os.PathSeparator))
	if ldx != -1 {
		localFileDir := localFilePath[:ldx]
		mkdirErr := os.MkdirAll(localFileDir, 0775)
		if mkdirErr != nil {
			err = errors.New("mkdir error")
			log.Println("Error: mkdir all failed for", localFileDir, mkdirErr.Error())
			return
		}
	}

	fileURI, pErr := url.Parse(fileURL)
	if pErr != nil {
		err = errors.New("invalid file url")
		log.Println("Error: invalid file url", fileURL)
		return
	}

	reqHost := fileURI.Host

	downURL := fileURL
	if proxyHost != "" {
		//use proxy
		downURL = strings.Replace(fileURL, reqHost, proxyHost, -1)
	}

	//new request
	req, reqErr := http.NewRequest("GET", downURL, nil)
	if reqErr != nil {
		err = reqErr
		log.Println("Error: new request", fileKey, "failed by url", downURL, reqErr)
		return
	}
	if referer != "" {
		req.Header.Add("Referer", referer)
	}

	log.Println("Info: downloading", fileKey, "=>", localFilePath, "...")
	//set request host
	req.Host = reqHost
	resp, respErr := http.DefaultClient.Do(req)
	if respErr != nil {
		err = respErr
		log.Println("Error: download", fileKey, "failed by url", downURL, respErr)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		localFp, openErr := os.Create(localFilePath)
		if openErr != nil {
			err = openErr
			log.Println("Error: open local file", localFilePath, "failed", openErr.Error())
			return
		}
		defer localFp.Close()
		_, cpErr := io.Copy(localFp, resp.Body)
		if cpErr != nil {
			err = cpErr
			log.Println("Error: download", fileKey, "failed", cpErr.Error())
			return
		}
		log.Println("Info: download", fileKey, "=>", localFilePath, "success")
	} else {
		err = errors.New("download failed")
		log.Println("Error: download", fileKey, "failed by url", downURL, resp.Status)
		return
	}
	return
}
