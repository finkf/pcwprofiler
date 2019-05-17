module github.com/finkf/pcwprofiler

go 1.12

require (
	github.com/finkf/gofiler v0.1.1
	github.com/finkf/pcwgo/api v0.6.0
	github.com/finkf/pcwgo/db v0.9.0
	github.com/finkf/pcwgo/jobs v0.1.0
	github.com/finkf/pcwgo/service v0.0.0-00010101000000-000000000000
	github.com/go-sql-driver/mysql v1.4.1
	github.com/sirupsen/logrus v1.4.1
)

replace github.com/finkf/pcwgo/db => ../pcwgo/db

replace github.com/finkf/pcwgo/api => ../pcwgo/api

replace github.com/finkf/pcwgo/service => ../pcwgo/service
