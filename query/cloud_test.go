//go:build cloud

/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package query

import (
	"context"
	"testing"
	"time"

	"github.com/qiangli/dgraph2/dgraphapi"
	"github.com/qiangli/dgraph2/dgraphtest"
	"github.com/qiangli/dgraph2/x"
)

func TestMain(m *testing.M) {
	c, err := dgraphtest.NewDCloudCluster()
	x.Panic(err)

	dg, cleanup, err := c.Client()
	x.Panic(err)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	x.Panic(dg.LoginIntoNamespace(ctx, dgraphapi.DefaultUser, dgraphapi.DefaultPassword, x.RootNamespace))

	dc = c
	client.Dgraph = dg
	populateCluster(dc)
	m.Run()
}
