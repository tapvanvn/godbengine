package adapter

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

var ErrDBEngineNotAbsolutePath = errors.New("path is not absolute")
var ErrDBEngineNotADirectory = errors.New("Path is not a valid directory")

type FileClient struct {
	absolutePath string
}

func NewFileClient(absolutePath string) (*FileClient, error) {
	fileInfo, err := os.Stat(absolutePath)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, ErrDBEngineNotADirectory
	}
	client := &FileClient{}
	client.init(absolutePath)
	return client, nil
}

func (client *FileClient) init(absolutePath string) {

	client.absolutePath = absolutePath
	//TODO: format absolutePath
	//TODO: check absolutePath must be existed directory and have the read/write permission
}

func (client *FileClient) Read(path string) (*[]byte, error) {
	if !filepath.IsAbs(path) {

		return nil, ErrDBEngineNotAbsolutePath
	}
	realPath := fmt.Sprintf("%s%s", client.absolutePath, path)
	file, err := os.Open(realPath)
	if err != nil {

		return nil, err
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return &bytes, nil
}

func (client *FileClient) Write(path string, content *[]byte) error {
	if !filepath.IsAbs(path) {

		return ErrDBEngineNotAbsolutePath
	}
	realPath := fmt.Sprintf("%s%s", client.absolutePath, path)
	return ioutil.WriteFile(realPath, *content, 0755)
}

func (client *FileClient) Delete(path string) error {

	if !filepath.IsAbs(path) {

		return ErrDBEngineNotAbsolutePath
	}

	realPath := fmt.Sprintf("%s%s", client.absolutePath, path)
	return os.Remove(realPath)

}
