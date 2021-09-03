package engine

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

//FilterItem filter item
type dbFilterItem struct {
	Field    string
	Operator string
	Value    interface{}
}

type dbSortItem struct {
	Field     string
	Inscrease bool
}

//DBQuery query
type DBQuery struct {
	Collection string
	Fields     []dbFilterItem
	SelectOne  bool
	SortFields []dbSortItem
	paging     *DBQueryPage
	Signature  string //to identify an query
}

//DBQueryPage paging
type DBQueryPage struct {
	PageNum  int
	PageSize int
}

//MakeDBQuery make new dbquery
func MakeDBQuery(collection string, selectOne bool) DBQuery {

	query := DBQuery{
		Collection: collection,
		Fields:     []dbFilterItem{},
		SelectOne:  selectOne,
		SortFields: []dbSortItem{},
	}

	return query
}

//Filter add a filter to query
//value must be a string, number, bool
func (query *DBQuery) Filter(field string, compareOperator string, value interface{}) {

	filterItem := dbFilterItem{
		Field:    field,
		Operator: compareOperator,
		Value:    value,
	}
	valSignature := ""
	if test, err := json.Marshal(value); err == nil {
		valSignature = string(test)
	}

	query.Signature += fmt.Sprintf("[%s/%s/%s]", field, compareOperator, valSignature)

	//TODO: verify operator, value

	query.Fields = append(query.Fields, filterItem)

}

func (query *DBQuery) Sort(field string, insc bool) {

	sortItem := dbSortItem{Field: field, Inscrease: insc}

	query.SortFields = append(query.SortFields, sortItem)

	query.Signature += fmt.Sprintf("[%s/%t]", field, insc)

}

func (query *DBQuery) GetSignature() string {
	if len(query.Signature) < 256 {
		return query.Signature
	}

	hash := sha256.Sum256([]byte(query.Signature))
	return fmt.Sprintf("%x", hash[:])
}

//Paging paging
func (query *DBQuery) Paging(pageNum int, pageSize int) {

	query.paging = &DBQueryPage{PageNum: pageNum, PageSize: pageSize}
}

//GetPaging get paging info
func (query *DBQuery) GetPaging() *DBQueryPage {

	return query.paging
}
