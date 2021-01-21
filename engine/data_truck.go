package engine

//# All item in a truck must be process inside a transaction.
//# It's mean it must be all success or all fail.

//DataTruckItem wrap for document item
type DataTruckItem struct {
	Collection string
	Document   Document
}

//DataTruck data truck
type DataTruck struct {
	Items []DataTruckItem
}

//Append append a document to a truck
func (truck *DataTruck) Append(collection string, document Document) {

	truck.Items = append(truck.Items, DataTruckItem{Collection: collection, Document: document})
}

//CreateDataStruck create a data truck
func CreateDataStruck() DataTruck {

	return DataTruck{Items: []DataTruckItem{}}
}
