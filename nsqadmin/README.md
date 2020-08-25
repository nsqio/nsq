## nsqadmin

`nsqadmin` is a Web UI to view aggregated cluster stats in realtime and perform various
administrative tasks.

Read the [docs](https://nsq.io/components/nsqadmin.html)


## Local Development

### Dependencies

 1. Install [`go-bindata`](https://github.com/shuLhan/go-bindata)
 2. Install NodeJS 8.x (includes npm)

### Workflow

 1. `$ npm install`
 2. `$ ./gulp --series clean watch` or `$ ./gulp --series clean build`
 3. `$ go-bindata --debug --pkg=nsqadmin --prefix=static/build static/build/...`
 4. `$ go build ../apps/nsqadmin && ./nsqadmin`
 5. make changes (repeat step 4 only if you make changes to any Go code)
 6. `$ go-bindata --pkg=nsqadmin --prefix=static/build static/build/...`
 7. commit other changes and `bindata.go`
