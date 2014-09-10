package nsq

import "testing"

func TestConfigSet(t *testing.T) {
	c := NewConfig()
	if err := c.Set("not a real config value", struct{}{}); err == nil {
		t.Error("No error when setting an invalid value")
	}
	if err := c.Set("tls_v1", "lol"); err == nil {
		t.Error("No error when setting `tls_v1` to an invalid value")
	}
	if err := c.Set("tls_v1", true); err != nil {
		t.Errorf("Error setting `tls_v1` config. %v", err)
	}

	if err := c.Set("tls-insecure-skip-verify", true); err != nil {
		t.Errorf("Error setting `tls-insecure-skip-verify` config. %v", err)
	}
	if c.TlsConfig.InsecureSkipVerify != true {
		t.Errorf("Error setting `tls-insecure-skip-verify` config: %v", c.TlsConfig)
	}
}

func TestConfigValidate(t *testing.T) {
	c := NewConfig()
	if err := c.Validate(); err != nil {
		t.Error("initialized config is invalid")
	}
	c.DeflateLevel = 100
	if err := c.Validate(); err == nil {
		t.Error("no error set for invalid value")
	}

}
