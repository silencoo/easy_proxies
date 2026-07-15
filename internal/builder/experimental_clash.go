//go:build with_clash_api

package builder

import "github.com/sagernet/sing-box/option"

func buildExperimentalOptions() *option.ExperimentalOptions {
	return &option.ExperimentalOptions{
		ClashAPI: &option.ClashAPIOptions{
			ExternalController: "127.0.0.1:9092",
		},
	}
}
