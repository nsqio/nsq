## nsqadmin

`nsqadmin` is a Web UI to view aggregated cluster stats in realtime and perform various
administrative tasks.

Read the [docs](http://nsq.io/components/nsqadmin.html)

## Working Locally

 1. `$ npm install`
 2. `$ ./gulp clean watch` or `$ ./gulp clean build`
 3. `$ go-bindata --debug --pkg=nsqadmin --prefix=static/build static/build/...`
 4. `$ go build && ./nsqadmin`
 5. make changes (repeat step 5 if you make changes to any Go code)
 6. `$ go-bindata --pkg=nsqadmin --prefix=static/build static/build/...`
 7. commit other changes and `bindata.go`
