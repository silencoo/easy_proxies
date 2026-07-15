//go:build !with_clash_api

package builder

import "github.com/sagernet/sing-box/option"

func buildExperimentalOptions() *option.ExperimentalOptions {
	return nil
}
