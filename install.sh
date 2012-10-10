#
# this script will build and install the various NSQ binaries
#

# platform agnostic way to get the directory this script is in
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEST="/usr/local/bin"
STATIC_DEST="/usr/local/share"

if [ "$UID" -ne 0 ]; then
	SU="$(command -v su) -c"
	SUDO="$(command -v sudo)"
	# prefer sudo over su
	if [ "$SUDO" ]; then
		SU=$SUDO
	fi
else
	unset SU
fi

cd $DIR

echo "building nsqd..."
cd nsqd
go build
echo "   installing nsqd in $DEST"
$SU cp nsqd $DEST

echo "building nsqlookupd..."
cd ../nsqlookupd
go build
echo "   installing nsqlookupd in $DEST"
$SU cp nsqlookupd $DEST

echo "building nsqadmin..."
cd ../nsqadmin
go build
echo "   installing nsqadmin in $DEST"
$SU cp nsqadmin $DEST
echo "   installing nsqadmin templates in $STATIC_DEST/nsqadmin/templates"
$SU mkdir -p $STATIC_DEST/nsqadmin
$SU cp -R templates $STATIC_DEST/nsqadmin

cd ../examples
for example_app in *; do
    echo "building examples/$example_app..."
    pushd $example_app >/dev/null
    go build
    echo "   installing $example_app in $DEST"
    $SU cp $example_app $DEST
    popd >/dev/null
done
