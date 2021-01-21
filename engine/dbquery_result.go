package engine

//DBQueryResult return query result
type DBQueryResult interface {
	Error() error
	Next(document interface{}) error
	GetOne(document interface{}) error
	Close()
	IsAvailable() bool
}
