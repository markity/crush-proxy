package comm

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
)

const BadGatewayString = "HTTP/1.1 502 Bad Gateway\r\n" +
	"Content-Length: 11\r\n" +
	"Content-Type: text/plain\r\n" +
	"\r\n" +
	"Bad Gateway"

const EstablishedString = "HTTP/1.1 200 Connection Established\r\n\r\n"

func BuildRawRequestHeader(req *http.Request) ([]byte, error) {
	if req == nil {
		return nil, fmt.Errorf("nil request")
	}

	var buf bytes.Buffer

	// 1. 构造请求目标，比如 /api/user?id=1
	target := "/"
	if req.URL != nil {
		target = req.URL.RequestURI()
		if target == "" {
			target = "/"
		}
	}

	// 2. 构造协议版本
	proto := req.Proto
	if proto == "" {
		proto = "HTTP/1.1"
	}

	// 3. 请求行：GET /xxx HTTP/1.1
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}

	fmt.Fprintf(&buf, "%s %s %s\r\n", method, target, proto)

	// 4. Host 头比较特殊，Go 里面 Host 通常在 req.Host，不在 req.Header 里
	host := req.Host
	if host == "" && req.URL != nil {
		host = req.URL.Host
	}

	if host != "" {
		fmt.Fprintf(&buf, "Host: %s\r\n", host)
	}

	// 5. 普通 Header
	for key, values := range req.Header {
		// Host 不从 req.Header 里写，避免重复
		if strings.EqualFold(key, "Host") {
			continue
		}

		// Content-Length 单独处理，避免重复或不准确
		if strings.EqualFold(key, "Content-Length") {
			continue
		}

		// Transfer-Encoding 单独处理
		if strings.EqualFold(key, "Transfer-Encoding") {
			continue
		}

		for _, value := range values {
			fmt.Fprintf(&buf, "%s: %s\r\n", key, value)
		}
	}

	// 6. Content-Length
	// req.ContentLength >= 0 时表示已知长度
	if req.ContentLength >= 0 {
		// GET / HEAD 通常可以没有 Content-Length
		// 但如果原请求明确有 body，或者 ContentLength > 0，就写
		if req.ContentLength > 0 {
			fmt.Fprintf(&buf, "Content-Length: %d\r\n", req.ContentLength)
		}
	}

	// 7. Transfer-Encoding，比如 chunked
	for _, te := range req.TransferEncoding {
		if te != "" {
			fmt.Fprintf(&buf, "Transfer-Encoding: %s\r\n", te)
		}
	}

	// 8. Connection: close
	if req.Close {
		fmt.Fprintf(&buf, "Connection: close\r\n")
	}

	// 9. 空行，表示 header 结束
	buf.WriteString("\r\n")

	return buf.Bytes(), nil
}

func BuildRawResponseHeader(resp *http.Response) ([]byte, error) {
	if resp == nil {
		return nil, fmt.Errorf("nil response")
	}

	var buf bytes.Buffer

	// 1. 构造协议版本
	proto := resp.Proto
	if proto == "" {
		proto = "HTTP/1.1"
	}

	// 2. 构造状态码和状态文本
	statusCode := resp.StatusCode
	if statusCode == 0 {
		statusCode = http.StatusOK
	}

	status := resp.Status
	if status == "" {
		statusText := http.StatusText(statusCode)
		if statusText != "" {
			status = fmt.Sprintf("%d %s", statusCode, statusText)
		} else {
			status = fmt.Sprintf("%d", statusCode)
		}
	}

	// 3. 状态行：HTTP/1.1 200 OK
	fmt.Fprintf(&buf, "%s %s\r\n", proto, status)

	// 4. 普通 Header
	for key, values := range resp.Header {
		// Content-Length 单独处理，避免重复或不准确
		if strings.EqualFold(key, "Content-Length") {
			continue
		}

		// Transfer-Encoding 单独处理
		if strings.EqualFold(key, "Transfer-Encoding") {
			continue
		}

		// Connection 单独处理，避免和 resp.Close 重复
		if strings.EqualFold(key, "Connection") {
			continue
		}

		for _, value := range values {
			fmt.Fprintf(&buf, "%s: %s\r\n", key, value)
		}
	}

	// 5. Content-Length
	// resp.ContentLength >= 0 表示已知长度
	if resp.ContentLength >= 0 {
		fmt.Fprintf(&buf, "Content-Length: %d\r\n", resp.ContentLength)
	}

	// 6. Transfer-Encoding，比如 chunked
	for _, te := range resp.TransferEncoding {
		if te != "" {
			fmt.Fprintf(&buf, "Transfer-Encoding: %s\r\n", te)
		}
	}

	// 7. Connection: close
	if resp.Close {
		fmt.Fprintf(&buf, "Connection: close\r\n")
	}

	// 8. 空行，表示 header 结束
	buf.WriteString("\r\n")

	return buf.Bytes(), nil
}
