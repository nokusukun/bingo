package bingo

type Document struct {
	ID string `json:"_id" bingo_json:"_id"`
}

func (d Document) Key() []byte {
	return []byte(d.ID)
}
