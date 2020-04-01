package errors

import "fmt"

type CompoundedError interface {
	GetErrors() []error
	Append(errs ...error)
	Error() string
}

type compoundedError struct {
	errs []error
}

func (c *compoundedError) GetErrors() []error {
	return c.errs
}

func (c *compoundedError) Append(errs ...error) {
	c.errs = append(c.errs, filerNilErrors(errs...)...)
}

func (c *compoundedError) Error() string {
	if len(c.errs) == 1 {
		return fmt.Sprint(c.errs[0])
	}

	return fmt.Sprint(c.errs)
}

func NewCompoundedError(errs ...error) CompoundedError {
	if errs == nil || len(errs) == 0 {
		return nil
	}

	real_errors := filerNilErrors(errs...)
	if real_errors == nil || len(real_errors) == 0 {
		return nil
	}

	return &compoundedError{errs: real_errors}
}

func NewCompoundedErrorFromChan(errCh <-chan error) CompoundedError {
	if errCh == nil || len(errCh) == 0 {
		return nil
	}

	errs := []error{}
	for err := range errCh {
		errs = append(errs, err)
	}

	return NewCompoundedError(errs...)
}

func filerNilErrors(errs ...error) []error {
	real_errors := []error{}
	for _, err := range errs {
		if err != nil {
			real_errors = append(real_errors, err)
		}
	}
	return real_errors
}
