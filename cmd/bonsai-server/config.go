/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Config layering: CLI flags (pflag) > env vars (BONSAI_*) > YAML config file
 * > flag defaults. After flag.Parse(), loadConfig binds every flag to viper,
 * reads the optional YAML, and writes any env/YAML value back into the flag
 * unless the user set it explicitly on the command line. Downstream code keeps
 * dereferencing the flag pointers it already has.
 */

package main

import (
	"fmt"
	"strings"

	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func loadConfig(configFile string) error {
	v := viper.New()
	v.SetEnvPrefix("BONSAI")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	v.AutomaticEnv()

	if err := v.BindPFlags(flag.CommandLine); err != nil {
		return fmt.Errorf("bind pflags: %w", err)
	}

	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return fmt.Errorf("read %s: %w", configFile, err)
		}
	}

	// For each flag the user did NOT pass on the command line, copy whichever
	// value viper resolved (env or YAML) into the flag. We skip the flags the
	// user changed so explicit CLI args still win.
	explicit := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { explicit[f.Name] = true })

	var visitErr error
	flag.VisitAll(func(f *flag.Flag) {
		if visitErr != nil || explicit[f.Name] {
			return
		}
		if !v.IsSet(f.Name) {
			return
		}
		if err := f.Value.Set(v.GetString(f.Name)); err != nil {
			visitErr = fmt.Errorf("apply %s: %w", f.Name, err)
		}
	})
	return visitErr
}
