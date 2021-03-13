package engine

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
}

//DBQueryPage paging
type DBQueryPage struct {
	PageNum  int
	PageSize int
}

//MakeDBQuery make new dbquery
func MakeDBQuery(collection string, selectOne bool) DBQuery {

	query := DBQuery{Collection: collection,
		Fields: []dbFilterItem{}, SelectOne: selectOne, SortFields: []dbSortItem{}}

	return query
}

//Filter add a filter to query
//value must be a string, number, bool
func (query *DBQuery) Filter(field string, compareOperator string, value interface{}) {

	filterItem := dbFilterItem{Field: field,
		Operator: compareOperator,
		Value:    value}

	//TODO: verify operator, value

	query.Fields = append(query.Fields, filterItem)
}

func (query *DBQuery) Sort(field string, insc bool) {

	sortItem := dbSortItem{Field: field, Inscrease: insc}

	query.SortFields = append(query.SortFields, sortItem)
}

//Paging paging
func (query *DBQuery) Paging(pageNum int, pageSize int) {

	query.paging = &DBQueryPage{PageNum: pageNum, PageSize: pageSize}
}

//GetPaging get paging info
func (query *DBQuery) GetPaging() *DBQueryPage {

	return query.paging
}
