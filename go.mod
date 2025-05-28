module go.mau.fi/whatsmeow

go 1.23.0

toolchain go1.24.3

require (
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.0
	github.com/rs/zerolog v1.34.0
	go.mau.fi/libsignal v0.2.0
	go.mau.fi/util v0.8.7
	golang.org/x/crypto v0.38.0
	golang.org/x/net v0.40.0
	google.golang.org/protobuf v1.36.6
)

require (
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/petermattis/goid v0.0.0-20250508124226-395b08cebbdb // indirect
	golang.org/x/exp v0.0.0-20250506013437-ce4c2cf36ca6 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/text v0.25.0 // indirect
)

replace go.mau.fi/util => ./go-util
