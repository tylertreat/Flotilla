package stomp

// Validator is an interface for validating STOMP frames.
type Validator interface {
	// Validate returns nil if the frame is valid, or an error if not valid.
	Validate(f *Frame) error
}

func NewValidator(version Version) Validator {
	return validatorNull{}
}

type validatorNull struct{}

func (v validatorNull) Validate(f *Frame) error {
	return nil
}
