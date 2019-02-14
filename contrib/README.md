## Optional/Contrib Modules

Contrib modules are a way to add functionality to nsqd, in a decoupled way.


The modules currently available are:

- Datadog


### Architecture

Contrib modules are initialized by passing in `--mod-opt=` to nsqd.  This may 
be provided multiple times.  An array of `mod-opt`s are then passed to the 
contrib module initializer (during nsqd initialization).  Each module is then
passed its options to see if valid options were provided, after which it is 
initialized and added to the nsqd waitGroup.


### Datadog

Datadog contrib module, reports nsqd statistics to a datadog daemon.  The options
it exposes are:

- `--mod-opt=-dogstatsd-address=<IP:PORT>`
- `--mod-opt=-dogstatsd-interval=<INT_SECONDS>`
- `--mod-opt=-dogstatsd-prefix=<STRING>`

