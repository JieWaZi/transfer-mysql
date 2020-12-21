package models

type RowRequest struct {
	Schema    string
	Name      string
	Action    string
	OldRows   []interface{}
	NewRows   []interface{}
	Timestamp uint32
}

type PosRequest struct {
	Name  string
	Pos   uint32
	Force bool
}