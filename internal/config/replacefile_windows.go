//go:build windows

package config

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var replaceFileW = windows.NewLazySystemDLL("kernel32.dll").NewProc("ReplaceFileW")

func atomicReplaceFile(source, destination string) error {
	if err := restrictReplacementDACL(source); err != nil {
		return err
	}
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	if _, err := os.Stat(destination); err == nil {
		// ReplaceFileW preserves the destination's DACL and other metadata. Do
		// not fall back to MoveFileEx on failure: doing so could silently replace
		// a restrictive ACL with the temporary file's inherited ACL.
		result, _, callErr := replaceFileW.Call(
			uintptr(unsafe.Pointer(to)),
			uintptr(unsafe.Pointer(from)),
			0,
			0,
			0,
			0,
		)
		if result == 0 {
			if callErr != nil && !errors.Is(callErr, syscall.Errno(0)) {
				return callErr
			}
			return syscall.EINVAL
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return windows.MoveFileEx(
		from,
		to,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func restrictReplacementDACL(path string) error {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return err
	}
	administrators, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		return err
	}
	entries := make([]windows.EXPLICIT_ACCESS, 0, 3)
	for _, sid := range []*windows.SID{user.User.Sid, system, administrators} {
		entries = append(entries, windows.EXPLICIT_ACCESS{
			AccessPermissions: windows.ACCESS_MASK(windows.GENERIC_ALL),
			AccessMode:        windows.GRANT_ACCESS,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_USER,
				TrusteeValue: windows.TrusteeValueFromSID(sid),
			},
		})
	}
	acl, err := windows.ACLFromEntries(entries, nil)
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

func syncParentDirectory(string) error { return nil }
