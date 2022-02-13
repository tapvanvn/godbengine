package adapter

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var ErrDBEngineNotAbsolutePath = errors.New("path is not absolute")
var ErrDBEngineNotADirectory = errors.New("Path is not a valid directory")
var ErrDBEngineInvalidPath = errors.New("invalid path")

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
	parts := strings.Split(path, "/")
	stack := []string{}
	needCreate := []string{}
	for _, part := range parts[:len(parts)-1] {
		part = strings.TrimSpace(part)
		if len(part) > 0 {
			switch part {
			case ".":
				break
			case "..":
				if len(stack) == 0 {
					return ErrDBEngineInvalidPath
				}
				stack = stack[:len(stack)-1]
				break
			default:
				stack = append(stack, part)
				checkPath := client.absolutePath + "/" + strings.Join(stack, "/")
				if file, err := os.Open(checkPath); err != nil {
					if os.IsNotExist(err) {
						needCreate = append(needCreate, checkPath)
					} else {
						return err
					}
				} else {
					file.Close()
				}
			}
		}
	}

	for _, path := range needCreate {

		if err := os.Mkdir(path, 0766); err != nil {
			return err
		}
	}

	realPath := fmt.Sprintf("%s%s", client.absolutePath, path)

	fmt.Println("real path:", realPath)

	return ioutil.WriteFile(realPath, *content, 0755)
}

func (client *FileClient) Delete(path string) error {

	if !filepath.IsAbs(path) {

		return ErrDBEngineNotAbsolutePath
	}

	realPath := fmt.Sprintf("%s%s", client.absolutePath, path)
	return os.Remove(realPath)

}
