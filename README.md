# stargo

```shell
window dev
配置 golang 环境
go version go1.18.9 windows/amd64
go env -w CGO_ENABLED=0 GOOS=linux GOARCH=amd64
go build

```


> repo.yaml 中配置 `repo: "file:///home/sr-dev/.stargo/test"` 可以使用本地