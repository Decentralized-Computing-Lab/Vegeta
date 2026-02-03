module go.themix.io/acs

go 1.16

require (
	go.themix.io/crypto v0.0.0-00010101000000-000000000000
	go.themix.io/transport v0.0.0-00010101000000-000000000000
	go.uber.org/zap v1.16.0
)

replace go.themix.io/transport => ../transport

replace go.themix.io/crypto => ../crypto
