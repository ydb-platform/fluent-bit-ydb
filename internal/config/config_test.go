package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseParamCredentialsStaticValue(t *testing.T) {
	for _, tt := range []struct {
		url      string
		user     string
		password string
		endpoint string
		err      bool
	}{
		{
			url:      "user:password@endpoint:2135",
			user:     "user",
			password: "password",
			endpoint: "endpoint:2135",
			err:      false,
		},
		{
			url:      "user@endpoint:2135",
			user:     "user",
			password: "",
			endpoint: "endpoint:2135",
			err:      false,
		},
		{
			url:      "user:password",
			user:     "user",
			password: "password",
			endpoint: "",
			err:      false,
		},
	} {
		t.Run("", func(t *testing.T) {
			user, password, endpoint, err := parseParamCredentialsStaticValue(tt.url)
			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.user, user)
				require.Equal(t, tt.password, password)
				require.Equal(t, tt.endpoint, endpoint)
			}
		})
	}
}
