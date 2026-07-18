//go:build windows

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWriteFileAtomicPreservesExistingWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.yaml")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := setCurrentUserOnlyDACL(path); err != nil {
		t.Fatalf("set restrictive DACL: %v", err)
	}
	before := windowsSecurityDescriptor(t, path)

	if err := WriteFileAtomic(path, []byte("new"), 0o600); err != nil {
		t.Fatalf("atomic write: %v", err)
	}
	after := windowsSecurityDescriptor(t, path)
	if after != before {
		t.Fatalf("destination security descriptor changed\nbefore: %s\nafter:  %s", before, after)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "new" {
		t.Fatalf("replacement contents: err=%v data=%q", err, data)
	}
}

func TestWriteFileAtomicProtectsNewWindowsDACL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new-credentials.yaml")
	if err := WriteFileAtomic(path, []byte("secret"), 0o600); err != nil {
		t.Fatalf("atomic write: %v", err)
	}
	descriptor := windowsSecurityDescriptor(t, path)
	if !strings.Contains(descriptor, "D:P") {
		t.Fatalf("new file DACL is not protected: %s", descriptor)
	}
	if strings.Contains(descriptor, ";;;WD)") {
		t.Fatalf("new file grants access to Everyone: %s", descriptor)
	}
}

func setCurrentUserOnlyDACL(path string) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{{
		AccessPermissions: windows.ACCESS_MASK(windows.GENERIC_ALL),
		AccessMode:        windows.GRANT_ACCESS,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  windows.TRUSTEE_IS_USER,
			TrusteeValue: windows.TrusteeValueFromSID(user.User.Sid),
		},
	}}, nil)
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		acl,
		nil,
	)
}

func windowsSecurityDescriptor(t *testing.T, path string) string {
	t.Helper()
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|
			windows.GROUP_SECURITY_INFORMATION|
			windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatalf("read security descriptor: %v", err)
	}
	return descriptor.String()
}
