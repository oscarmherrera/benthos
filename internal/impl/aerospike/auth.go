package aerospike

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/benthosdev/benthos/v4/public/service"
)

func authDescription() string {
	return `### Authentication
Aerospike supports many different authentication mechanisms, including LDAP, internal, and external authentication.
for the Bethos components we will be using either TLS or mutalTLS depending on the configuration presented.
You must ensure that the server can authenticate the client and the client can authenticate the server.
`
}

func authFieldSpec() *service.ConfigField {
	return service.NewObjectField("auth",
		service.NewIntField("auth_mode").
			Description("The authentication mode for the aerospike cluster, 0=AuthModeInternal,1=AuthModeExternal,2=AuthModePKI").
			Example("1").
			Default(0).
			Optional(),
		service.NewStringField("username").
			Description("The username for the aerospike cluster").
			Example("admin").
			Optional(),
		service.NewStringField("password").
			Description("The password for the aerospike cluster").
			Example("admin").
			Secret().
			Optional(),
		service.NewStringField("user_certificate").
			Description("The user certificate that will be used to authenticate to the aerospike cluster").
			Example("/path/to/user.crt").
			Optional(),
	).Description(" configuration for Aerospike authentication parameters.").
		Advanced()
}

type authConfig struct {
	AuthMode        int
	Username        string
	UserPassword    string
	UserCertificate string
}

//------------------------------------------------------------------------------

// AuthFromParsedConfig attempts to extract an auth config from a ParsedConfig.
func AuthFromParsedConfig(p *service.ParsedConfig) (c authConfig, err error) {
	if p.Contains("auth_mode") {
		if c.AuthMode, err = p.FieldInt("auth_mode"); err != nil {
			return
		}
		if c.AuthMode == 0 || c.AuthMode == 1 {
			if !p.Contains("username") || !p.Contains("password") {
				err = errors.New("missing auth.username or auth.password config field for authentication mode 0 or 1")
				return
			}
		}
		if c.AuthMode == 2 {
			if !p.Contains("user_certificate") {
				err = errors.New("missing auth.user_certificate config field for authentication mode 2")
				return
			}
		}

	}

	if p.Contains("username") {
		if c.Username, err = p.FieldString("username"); err != nil {
			return
		}
	}
	if p.Contains("password") {
		if c.UserPassword, err = p.FieldString("password"); err != nil {
			return
		}
	}
	if p.Contains("user_certificate") {
		if c.UserCertificate, err = p.FieldString("user_certificate"); err != nil {
			err = errors.New("missing auth.certificate config field")
			return

		}
	}
	return
}

//func userJWTHandler(filename string, fs *service.FS) nats.UserJWTHandler {
//	return func() (string, error) {
//		contents, err := loadFileContents(filename, fs)
//		if err != nil {
//			return "", err
//		}
//		defer wipeSlice(contents)
//
//		return nkeys.ParseDecoratedJWT(contents)
//	}
//}

func sigHandler(filename string, fs *service.FS) nats.SignatureHandler {
	return func(nonce []byte) ([]byte, error) {
		contents, err := loadFileContents(filename, fs)
		if err != nil {
			return nil, err
		}
		defer wipeSlice(contents)

		kp, err := nkeys.ParseDecoratedNKey(contents)
		if err != nil {
			return nil, fmt.Errorf("unable to extract key pair from file %q: %v", filename, err)
		}
		defer kp.Wipe()

		sig, _ := kp.Sign(nonce)
		return sig, nil
	}
}

// Just wipe slice with 'x', for clearing contents of creds or nkey seed file.
func wipeSlice(buf []byte) {
	for i := range buf {
		buf[i] = 'x'
	}
}

func expandPath(p string) (string, error) {
	p = os.ExpandEnv(p)

	if !strings.HasPrefix(p, "~") {
		return p, nil
	}

	home, err := homeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, p[1:]), nil
}

func homeDir() (string, error) {
	if runtime.GOOS == "windows" {
		homeDrive, homePath := os.Getenv("HOMEDRIVE"), os.Getenv("HOMEPATH")
		userProfile := os.Getenv("USERPROFILE")

		var home string
		if homeDrive == "" || homePath == "" {
			if userProfile == "" {
				return "", errors.New("nats: failed to get home dir, require %HOMEDRIVE% and %HOMEPATH% or %USERPROFILE%")
			}
			home = userProfile
		} else {
			home = filepath.Join(homeDrive, homePath)
		}

		return home, nil
	}

	home := os.Getenv("HOME")
	if home == "" {
		return "", errors.New("nats: failed to get home dir, require $HOME")
	}
	return home, nil
}

func loadFileContents(filename string, fs *service.FS) ([]byte, error) {
	path, err := expandPath(filename)
	if err != nil {
		return nil, err
	}

	f, err := fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return io.ReadAll(f)
}
