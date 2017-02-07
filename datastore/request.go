package datastore

type RequestType int


type Request struct {
	Name string
	Type RequestType
	Args [][]byte
	//func GetType (RequestType)
	//func GetKey ([]byte)

}