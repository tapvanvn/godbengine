package engine

//FilePool provide file store service
type FilePool interface {
	Read(path string) (*[]byte, error)
	Write(path string, content *[]byte) error
	Delete(path string) error
}
