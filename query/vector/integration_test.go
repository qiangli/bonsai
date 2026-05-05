//go:build integration

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package query

import (
	"context"
	"testing"

	"github.com/qiangli/dgraph2/dgraphapi"
	"github.com/qiangli/dgraph2/dgraphtest"
	"github.com/qiangli/dgraph2/x"
)

func TestMain(m *testing.M) {
	dc = dgraphtest.NewComposeCluster()

	var err error
	var cleanup func()
	client, cleanup, err = dc.Client()
	x.Panic(err)
	defer cleanup()
	x.Panic(client.LoginIntoNamespace(context.Background(), dgraphapi.DefaultUser,
		dgraphapi.DefaultPassword, x.RootNamespace))

	m.Run()
}
