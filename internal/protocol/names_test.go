package protocol

import (
    "testing"
)


// Test generated using Keploy
func TestIsValidTopicName_ValidName_ReturnsTrue(t *testing.T) {
    validName := "valid_topic_name"
    result := IsValidTopicName(validName)
    if !result {
        t.Errorf("Expected true, got false for valid topic name: %s", validName)
    }
}

// Test generated using Keploy
func TestIsValidTopicName_TooLongName_ReturnsFalse(t *testing.T) {
    longName := "this_is_a_very_long_topic_name_that_exceeds_the_sixty_four_character_limit"
    result := IsValidTopicName(longName)
    if result {
        t.Errorf("Expected false, got true for too long topic name: %s", longName)
    }
}


// Test generated using Keploy
func TestIsValidChannelName_ValidName_ReturnsTrue(t *testing.T) {
    validName := "valid_channel_name"
    result := IsValidChannelName(validName)
    if !result {
        t.Errorf("Expected true, got false for valid channel name: %s", validName)
    }
}

