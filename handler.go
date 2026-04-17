package netx

type Handler interface {
	// Handle 请求处理函数
	Handle(r Request, w ResponseWriter)
}

type Request interface {
	// Id 请求id
	Id() uint32
	// Scanner 返回字节流扫描器
	Scanner() *Scanner
	// Read 读请求字节流
	Read(p []byte) (n int, err error)
	// Close 关闭请求字节流，并释放请求
	Close() error
	// BindJSON 请求流绑定到JSON对象
	BindJSON(a any) error
	// BindAny 绑定到实现了 Decoder 接口的任意对象
	BindAny(a Decoder) error
}

type Response interface {
	// Id 相应Id
	Id() uint32
	// Scanner 返回字节流扫描器
	Scanner() *Scanner
	// Read 读取响应流
	Read(p []byte) (n int, err error)
	// Close 关闭请求字节流，并释放响应流
	Close() error
}

type ResponseWriter interface {
	iRequestResponseWriter
}

type Encoder interface {
	Encode() ([]byte, error)
}

type Decoder interface {
	Decode([]byte) error
}
