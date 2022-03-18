package service

import (
	exporter_repo "github.com/wwqdrh/datamanager/domain/exporter/repository"
	stream_repo "github.com/wwqdrh/datamanager/domain/stream/repository"
	stream_service "github.com/wwqdrh/datamanager/domain/stream/service/fulltrigger"
)

func InitService() {
	exporter_repo.InitRepo()
	stream_repo.InitRepo()
	stream_service.MetaService.Init()
}
