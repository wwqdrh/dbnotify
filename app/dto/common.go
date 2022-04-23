package dto

import (
	"encoding/json"
	"net/http"
)

type BaseResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type JSONData struct {
	No   int         `json:"no"`
	Data interface{} `json:"data"`
}

// JSON 返回json数据，自动识别jsonp
func JSON(w http.ResponseWriter, statusCode int, data interface{}) error {
	w.WriteHeader(statusCode)
	w.Header().Set("Content-Type", "application/json")
	bs, err := json.Marshal(data)
	if err != nil {
		return err
	}
	w.Write(bs)
	return nil
}

// JSON2 返回json数据，自动识别jsonp
func JSON2(w http.ResponseWriter, statusCode int, no int, data interface{}) error {
	return JSON(w, statusCode, &JSONData{No: no, Data: data})
}
