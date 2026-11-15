# rityak/http

Module path: `github.com/metacubex/http`. Base: MetaCubeX/http v0.1.8
(`3ffb994bb87614dd9f1fc40e036613b04b4a8ffd`). The upstream BSD license is retained.

The HTTP/2 server exposes two opt-in capabilities needed by HTTP CONNECT relays:

- `Server.EnableExtendedConnectProtocol` enables RFC 8441 SETTINGS and request
  acceptance on that server, without changing process-wide `GODEBUG`.
- The response writer implements `CloseWrite() error`: sends END_STREAM while
  keeping the request body readable. The caller serializes CloseWrite with Write
  and Flush. Returning from the handler still terminates unfinished uploads.
  Stream state transitions accept DATA and trailers after the response FIN and
  release the stream after both directions finish.

Tests cover independent server configuration, upload after response EOF, a
neighboring request, repeated CloseWrite, rejected writes after FIN, and handler
return with an unfinished request. No client behavior or module path is changed.
