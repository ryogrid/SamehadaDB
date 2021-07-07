package buffer

import (
	"testing"

	"github.com/brunocalza/go-bustub/testingutils"
)

func TestClockReplacer(t *testing.T) {
	clockReplacer := NewClockReplacer(7)

	// Scenario: unpin six elements, i.e. add them to the replacer.
	clockReplacer.Unpin(1)
	clockReplacer.Unpin(2)
	clockReplacer.Unpin(3)
	clockReplacer.Unpin(4)
	clockReplacer.Unpin(5)
	clockReplacer.Unpin(6)
	clockReplacer.Unpin(1)
	testingutils.Equals(t, 6, clockReplacer.Size())

	// Scenario: get three victims from the clock.
	var value *FrameID
	value = clockReplacer.Victim()
	testingutils.Equals(t, FrameID(1), *value)
	value = clockReplacer.Victim()
	testingutils.Equals(t, FrameID(2), *value)
	value = clockReplacer.Victim()
	testingutils.Equals(t, FrameID(3), *value)

	// Scenario: pin elements in the replacer.
	// Note that 3 has already been victimized, so pinning 3 should have no effect.
	clockReplacer.Pin(3)
	clockReplacer.Pin(4)
	testingutils.Equals(t, 2, clockReplacer.Size())

	// Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
	clockReplacer.Unpin(4)

	// Scenario: continue looking for victims. We expect these victims.
	value = clockReplacer.Victim()
	testingutils.Equals(t, FrameID(5), *value)
	value = clockReplacer.Victim()
	testingutils.Equals(t, FrameID(6), *value)
	value = clockReplacer.Victim()
	testingutils.Equals(t, FrameID(4), *value)
}
