package core

type CoreOption func() error

func Init(o ...CoreOption) {
	var err error
	for _, i := range o {
		if err = i(); err != nil {
			panic(err)
		}
	}
}
