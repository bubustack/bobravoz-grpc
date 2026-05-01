package hub

import (
	"fmt"

	tractatusvalidation "github.com/bubustack/tractatus/validation"
	"google.golang.org/protobuf/proto"
)

func validateTransportMessage(kind string, msg proto.Message) error {
	if msg == nil {
		return nil
	}
	if err := tractatusvalidation.Validate(msg); err != nil {
		return fmt.Errorf("transport %s invalid: %w", kind, err)
	}
	return nil
}
