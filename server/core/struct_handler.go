package core

import (
	"datamanager/server/common/structhandler"
	"datamanager/server/global"
)

func InitStructHandler(handler ...structhandler.IStructHandler) CoreOption {
	return func() error {
		if len(handler) == 0 || handler[0] == nil {
			if global.G_StructHandler == nil {
				global.G_StructHandler = structhandler.NewBaseStructHandler(global.G_DATADB)
			}
		} else {
			global.G_StructHandler = handler[0]
		}
		return nil
	}
}
