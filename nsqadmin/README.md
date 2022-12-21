## nsqadmin

`nsqadmin` is a Web UI to view aggregated cluster stats in realtime and perform various
administrative tasks.

Read the [docs](https://nsq.io/components/nsqadmin.html)


## Local Development

### Dependencies

 1. Install NodeJS 16.x (includes `npm`)

### Live Reload Workflow

 1. `$ npm install`
 2. `$ ./gulp --series clean watch`
 3. `$ cd .. && make && ./build/nsqadmin --dev-static-dir=nsqadmin/static/build --lookupd-http-address=<...>`
 4. make changes to static assets (repeat step 3 only if you make changes to any Go code)

### Build

 1. `$ ./gulp --series clean build`
