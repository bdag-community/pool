package server

import "testing"

func TestSetNotifyCleanFlagPreservesTemplateFlagWithoutOverride(t *testing.T) {
	notify := []interface{}{"job", "prev", "version", "bits", "time", true}

	setNotifyCleanFlag(notify, preserveNotifyClean)

	if notify[5] != true {
		t.Fatalf("clean flag = %v, want preserved true", notify[5])
	}
}

func TestSetNotifyCleanFlagCanForceFalseForRoutineResend(t *testing.T) {
	notify := []interface{}{"job", "prev", "version", "bits", "time", true}

	setNotifyCleanFlag(notify, forceNotifyCleanFalse)

	if notify[5] != false {
		t.Fatalf("clean flag = %v, want false", notify[5])
	}
}

func TestSetNotifyCleanFlagCanForceTrueForRecoveryResend(t *testing.T) {
	notify := []interface{}{"job", "prev", "version", "bits", "time", false}

	setNotifyCleanFlag(notify, forceNotifyCleanTrue)

	if notify[5] != true {
		t.Fatalf("clean flag = %v, want true", notify[5])
	}
}
