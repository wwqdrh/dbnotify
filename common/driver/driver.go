package driver

func InitDriver() (errs []error) {

	if err := InitPostgresDriver(); err != nil {
		errs = append(errs, err)
	}
	if err := InitSqliteDriver(); err != nil {
		errs = append(errs, err)
	}

	return
}
