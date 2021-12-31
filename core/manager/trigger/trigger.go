package trigger

import "datamanager/pkg/plugger/postgres"

type ITriggerPolicy interface {
	postgres.ITriggerPolicy
}
