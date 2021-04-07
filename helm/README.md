# nsq helm chart

helm: v3

<br>

nsq helm chart:

- nsqlookupd
- nsqd
- nsqadmin


<br>
<br>


## 注意(Attention)

由于nsqd涉及数据落地，因此需要挂载卷。

nsqd helm chart中，默认启用了持久化，因此需要k8s集群有storageclass。

Nsqd needs persistence, so the k8s cluster needs storageclass. If not, please disable persistence and use emptyDir.


```yaml
# nsqd/values.yaml

# if persistence, make sure k8s cluster has storageclass
# if not persistence, use emptyDir
persistence:
  enabled: true
  # enabled: false
```

<br>

如果集群中没有存储类，而发布nsqd到集群，pvc会一直出于Pending状态，nsqd statefulset的副本数为0，会一直不可用。

如果k8s集群没有storageclass，则请修改`nsqd/values.yaml`来禁用持久化，使用emptyDir来挂载nsqd的卷。

```yaml
persistence:
  enabled: false
```


<br>
<br>


## helm

```bash
# 进入helm目录
cd helm/


# 相关命令
helm upgrade [RELEASE] [CHART] [flags]

# 安装
helm upgrade --install -n namespace release-name chart-name
# 测试 渲染k8s manifests
helm install --dry-run --debug -n namespace release-name chart-name


# deploy nsqlookupd
helm upgrade --install nsqlookupd nsqlookupd

# deploy nsqd
helm upgrade --install nsqd nsqd

# deploy nsqadmin
helm upgrade --install  nsqadmin nsqadmin
```


<br>
<br>


## Auth

由于nsqadmin web页面没有，所以需要使用nginx ingress auth来实现认证功能。

Nsqdadmin have not auth function, so we need nginx ingress to complete auth.

```bash
# auth
# file: auth
# user: nsqadmin
htpasswd -c auth  nsqadmin

# input password
random-passwordxxx


# write auth to k8s secret
kubectl create secret generic nsqadmin-auth --from-file=auth


# add ingress annotations
# you need to modify nsqadmin/values.yaml ingress
kind: Ingress
metadata:
  name: xxx
  annotations:
    # type of authentication
    nginx.ingress.kubernetes.io/auth-type: basic
    # name of the secret that contains the user/password definitions
    nginx.ingress.kubernetes.io/auth-secret: nsqadmin-auth
    # message to display with an appropriate context why the authentication is required
    nginx.ingress.kubernetes.io/auth-realm: 'Authentication Required - nsqadmin'
```
