package stream

import (
	stream_repo "github.com/wwqdrh/datamanager/domain/stream/repository"
)

func Register() {
	stream_repo.InitRepo()
}
