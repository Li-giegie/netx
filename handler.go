package netx

type Handler interface {
	// Handle 请求处理函数
	Handle(r *SessionReader, w *SessionWriter)
}
