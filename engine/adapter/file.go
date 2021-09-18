package adapter

type FileClient struct {
	absolutePath string
}

func (client *FileClient) init(absolutePath string) {
	client.absolutePath = absolutePath
}
