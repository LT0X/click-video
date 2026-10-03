package llm_test

import (
	"os"
	"testing"

	"douyin/package/llm"
)

func TestChatGPT(t *testing.T) {
	if os.Getenv("RUN_SPARK_INTEGRATION") != "1" {
		t.Skip("设置 RUN_SPARK_INTEGRATION=1 后运行真实 Spark API 集成测试")
	}
	content := "介绍一下美国"
	if answer := llm.RequestToSparkAPI(content); answer == "" {
		t.Fatal("Spark API 返回空响应")
	}
}
