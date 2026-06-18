package visit

//go:generate go run github.com/temporalio/temporal-proxy/cmd/codegen visitors -o payloads.go -t Payload
//go:generate go run github.com/temporalio/temporal-proxy/cmd/codegen visitors -o datablobs.go -t DataBlob
//go:generate go run github.com/temporalio/temporal-proxy/cmd/codegen visitors -o memos.go -t Memo
