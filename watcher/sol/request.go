package sol

import (
	"fmt"
	"watcher/logger"
	"watcher/types"
	"watcher/utils"

	"github.com/spf13/viper"
)

var SolonaRpcURL string

func GetSolanaRpcURL() string {
	if SolonaRpcURL != "" {
		return SolonaRpcURL
	}
	rpc := viper.GetString("sol.rpc")
	if rpc != "" {
		return rpc
	}
	return viper.GetString("sol.rpc-helius")
}

type SolanaRpcRequest struct {
	Jsonrpc string        `json:"jsonrpc"`
	ID      string        `json:"id"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
}

type SolanaRpcResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      string      `json:"id"`
	Result  interface{} `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func CallRpc(method string, params []interface{}) (interface{}, error) {
	url := GetSolanaRpcURL()

	req := SolanaRpcRequest{
		Jsonrpc: "2.0",
		ID:      "1",
		Method:  method,
		Params:  params,
	}

	var resp SolanaRpcResponse
	err := utils.PostUrlResponseWithRetry(url, req, &resp, utils.DefaultRetryTimes, logger.SolLogger)
	if err != nil {
		return nil, fmt.Errorf("RPC %s failed: %w", method, err)
	}

	if resp.Error != nil {
		return nil, fmt.Errorf("RPC %s returned error: %d %s", method, resp.Error.Code, resp.Error.Message)
	}

	return resp.Result, nil
}

func GetSlotLeaders(start, limit uint64) (types.SlotLeaders, error) {
	result, err := CallRpc("getSlotLeaders", []interface{}{start, limit})
	if err != nil {
		return nil, err
	}

	leaders, ok := result.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type for leaders: %T", result)
	}

	res := make(types.SlotLeaders, 0, len(leaders))
	for i, v := range leaders {
		str, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected type in leaders array: %T", v)
		}
		res = append(res, &types.SlotLeader{
			Slot:   start + uint64(i),
			Leader: str,
		})
	}

	return res, nil
}

func GetCurrentSlot() (uint64, error) {
	result, err := CallRpc("getSlot", []interface{}{map[string]string{"commitment": "finalized"}})
	if err != nil {
		return 0, err
	}

	slot, ok := result.(float64) // json.Unmarshal default decodes numbers as float64
	if !ok {
		return 0, fmt.Errorf("unexpected type for slot: %T", result)
	}

	return uint64(slot), nil
}
