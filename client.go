package netx

func NewClient(c *Conn, h Handler) *Client {
	return &Client{
		ConnX: &ConnX{
			idCounter: new(uint32),
			response:  newStreamManager(),
			conn:      c,
			h:         h,
		},
	}
}

type Client struct {
	*ConnX
}
