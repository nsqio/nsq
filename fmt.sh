#!/bin/bash
find . -name "*.go" | xargs goimports -w
