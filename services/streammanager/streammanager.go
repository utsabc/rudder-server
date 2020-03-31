package streammanager

import (
	"errors"
	"os"

	Kafkamanager "github.com/rudderlabs/rudder-server/services/streammanager/kafkamanager"
)

// StreamOutput is Output after publishing message
/* type StreamOutput struct {
	Message string
	Error   error
} */

// StreamManager inplements produce method.
type StreamManager interface {
	Produce(*os.File) (string, error)
	// Produce(*os.File, ...string) error
}

type SettingsT struct {
	Provider string
	Config   map[string]interface{}
}

func New(settings *SettingsT) (StreamManager, error) {

	switch settings.Provider {
	case "KAFKA":
		/* providerConfig := make(map[string]interface{})
		providerConfig["hostName"] = "localhost:9092"
		providerConfig["topic"] = "myTestTopic" */

		providerConfig := settings.Config
		km, err := Kafkamanager.New(providerConfig)
		return &km, err
	default:
		return nil, errors.New("No provider configured for StreamManager")
	}
}

/* func Setup(destType string) {

} */
