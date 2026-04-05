package any_req_resp

import "encoding/json"

type Api struct {
	Path   string
	Method string
	Param  map[string]string
}

func (a *Api) Encode() ([]byte, error) {
	return json.Marshal(a)
}

func (a *Api) Decode(b []byte) error {
	return json.Unmarshal(b, &a)
}
