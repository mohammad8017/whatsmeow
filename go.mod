module go.mau.fi/whatsmeow

go 1.24.0

toolchain go1.25.1

require (
	github.com/beeper/argo-go v1.1.2
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.0
	github.com/rs/zerolog v1.34.0
	go.mau.fi/libsignal v0.2.0
	go.mau.fi/util v0.9.1
	golang.org/x/crypto v0.42.0
	golang.org/x/net v0.44.0
	google.golang.org/protobuf v1.36.9
)

require (
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/elliotchance/orderedmap/v3 v3.1.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/petermattis/goid v0.0.0-20250904145737-900bdf8bb490 // indirect
	github.com/vektah/gqlparser/v2 v2.5.27 // indirect
	golang.org/x/exp v0.0.0-20250911091902-df9299821621 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace go.mau.fi/util => ./go-util
