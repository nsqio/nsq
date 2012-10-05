#
# this script will build and install the various NSQ binaries
#

# platform agnostic way to get the directory this script is in
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="/usr/local/bin"

cd $DIR

echo "building nsqd..."
cd nsqd
go build
echo "   installing nsqd in $DEST"
cp nsqd $DEST

echo "building nsqlookupd..."
cd ../nsqlookupd
go build
echo "   installing nsqlookupd in $DEST"
cp nsqlookupd $DEST

echo "building nsqadmin..."
cd ../nsqadmin
go build
echo "   installing nsqadmin in $DEST"
cp nsqadmin $DEST

cd ../examples
for example_app in *; do
    echo "building examples/$example_app..."
    pushd $example_app >/dev/null
    go build
    echo "   installing $example_app in $DEST"
    cp $example_app $DEST
    popd >/dev/null
done
