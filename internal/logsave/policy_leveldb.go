package logsave

import (
	"encoding/json"
	"errors"

	"github.com/wwqdrh/datamanager/internal/datautil"
	"github.com/wwqdrh/datamanager/internal/logsave/base"
)

type LogSaveLeveldb struct {
	Queue *datautil.Queue
}

func (l *LogSaveLeveldb) Write(data map[string]interface{}) error {
	var logs []base.LogTable
	{
		b, err := json.Marshal(data["data"])
		if err != nil {
			return err
		}
		json.Unmarshal(b, &logs)
	}
	var policys base.Policy
	{
		b, err := json.Marshal(data["policy"])
		if err != nil {
			return err
		}
		json.Unmarshal(b, &policys)
	}
	// json.Marshal()

	// primaryFields := strings.Split(policy.PrimaryFields, ",")
	// 	datar, err := s.Dump(data, primaryFields)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if datar == nil {
	// 		return nil // 监听字段未命中
	// 	}

	// 	if err = exporter_repo.LogRepoV2.Write(policy.TableName, datar, policy.Outdate, policy.MinLogNum); err != nil {
	// 		return err
	// 	}
	// 	return nil
	return errors.New("还未实现")
}
