package core

import (
	"github.com/wwqdrh/datamanager/internal/structhandler"
)

func InitStructHandler(handler ...structhandler.IStructHandler) CoreOption {
	return func() error {
		if len(handler) == 0 || handler[0] == nil {
			if G_StructHandler == nil {
				G_StructHandler = structhandler.NewBaseStructHandler(G_DATADB)
			}
		} else {
			G_StructHandler = handler[0]
		}
		return nil
	}
}
