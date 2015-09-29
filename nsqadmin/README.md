## nsqadmin

`nsqadmin` is a Web UI to view aggregated cluster stats in realtime and perform various
administrative tasks.

Read the [docs](http://nsq.io/components/nsqadmin.html)

## Working Locally

 1. `$ npm install -g gulp`
 2. `$ npm install`
 3. `$ gulp clean watch` or `$ gulp clean build`
 4. `$ go-bindata --debug --pkg=nsqadmin --prefix=static/build static/build/...`
 5. `$ cd ../nsqadmin && go build && ./nsqadmin`
 6. make changes (repeat step 5 if you make changes to any Go code)
 7. `$ go-bindata --pkg=nsqadmin --prefix=static/build static/build/...`
 8. commit other changes and `bindata.go`
