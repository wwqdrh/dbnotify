package core

func InitDomain(fn ...func()) CoreOption {
	return func() error {
		for _, item := range fn {
			item()
		}
		return nil
	}
}
