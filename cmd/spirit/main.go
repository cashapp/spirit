package main

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/squareup/spirit/pkg/migration"
)

var cli struct {
	migration.Migration `cmd:"" help:"Schema change tables"`
}

func main() {
	ctx := kong.Parse(&cli)
	err := ctx.Run()
	if err != nil {
		fmt.Errorf("%+v", err)
	}
	ctx.FatalIfErrorf(err)
}
